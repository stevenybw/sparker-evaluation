package org.apache.spark.pacman.example

import breeze.linalg.DenseVector
import breeze.stats.distributions.Rand
import org.apache.spark.SparkEnv
import org.apache.spark.sparkle.{SparkleContext, SplitAggregateMetric, ZmqCommunicator}

import scala.util.Random

/**
 * Benchmarks
 */
object Benchmark {
  /**
   * Measures the performance of [[org.apache.spark.storage.BlockBasedMessagePassingManager]]
   *
   * @param spc
   * @param nrt  the number of round-trips
   * @param size message size in the number of Long
   * @param verify
   * @param seed
   * @return the average round-trip time in second
   */
  def bmPingpong(spc: SparkleContext, nrt: Int = 512, size: Int = 128, verify: Boolean = false, seed: Long = 2019L): Double = {
    val rtts = spc.spawn(2, ctx => {
      val rank = ctx.rank
      val topo = ctx.topology
      val messagePassingManager = SparkEnv.get.messagePassingManager
      if (rank == 0) {
        // Sync and verify
        val rng = new Random(seed)
        val data = if (verify) Array.fill(size)(rng.nextLong()) else Array.fill(size)(0L)
        messagePassingManager.Isend(data, topo(1).executorId, topo(1).host, topo(1).port, "data")
        val result = messagePassingManager.Recvall[Array[Long]](Array((topo(1).executorId, "ack"))).take()
        require(result._1 == topo(1).executorId)
        require(result._2 == "ack")
        require(result._3.deep == data.deep)

        // Start data transmission
        val beginTime = System.nanoTime()
        for (i <- 0 until nrt) {
          messagePassingManager.Isend(data, topo(1).executorId, topo(1).host, topo(1).port, "data")
          val result = messagePassingManager.Recvall[Array[Long]](Array((topo(1).executorId, "ack"))).take()
          require(result._1 == topo(1).executorId)
          require(result._2 == "ack")
        }
        val endTime = System.nanoTime()
        1e-9 * (endTime - beginTime) / nrt.toDouble
      } else if (rank == 1) {
        for (i <- 0 until (nrt + 1)) {
          val result = messagePassingManager.Recvall[Array[Long]](Array((topo(0).executorId, "data"))).take()
          messagePassingManager.Isend(result._3, topo(0).executorId, topo(0).host, topo(0).port, "ack")
        }
        0.0
      } else {
        0.0
      }
    })
    rtts(0)
  }

  /**
   * Measures the latency of [[org.apache.spark.storage.BlockBasedMessagePassingManager]]
   *
   * @param spc
   */
  def bmMeasureLatency(spc: SparkleContext): Unit = {
    var size = 8
    val nrt = 128
    val maxSize = 1024
    while (size <= maxSize) {
      val rtt = bmPingpong(spc, nrt, size / 8)
      val latency = rtt / 2
      val mbps = 1e-6 * size / latency
      println(s"${size} bytes   ${1e6 * latency} us   ${mbps} MB/s")
      size *= 2
    }
  }

  /**
   * Perform allgather with random data and measure the time
   *
   * @param parallelism The parallelism of allgather
   *                    None: use single-thread allgather
   *                    Some(p): use multi-thread allgather
   * @return
   */
  def zmqDoAllgather(spc: SparkleContext, arrayBytes: Int, parallelism: Option[Int], numAttempts: Int = 2, seed: Int = 2019): (Int, Double, Double) = {
    val numExecutors = spc.sc.executorLocations.size
    val durations = spc.spawnZmq(comm => {
      val data = new Array[Byte](arrayBytes)
      val rand = new scala.util.Random(seed)
      rand.nextBytes(data)
      if (parallelism.isDefined) {
        comm.allgatherParallel(data, parallelism.get)
      } else {
        comm.allgather(data)
      }
      val beginTimeMs = System.currentTimeMillis()
      for (i <- 0 until numAttempts) {
        if (parallelism.isDefined) {
          comm.allgatherParallel(data, parallelism.get)
        } else {
          comm.allgather(data)
        }
      }
      val endTimeMs = System.currentTimeMillis()
      1.0 * (endTimeMs - beginTimeMs) / numAttempts
    })
    val avg_duration_ms = durations.sum / durations.size
    val avg_bw = 1e-3 * arrayBytes / (1.0 * avg_duration_ms / (2 * (numExecutors - 1)))
    (arrayBytes, avg_duration_ms, avg_bw)
  }

  /**
   * Perform ReduceScatter with random data and measure the time
   *
   * @param parallelism The parallelism
   *                    None: use single-thread version
   *                    Some(p): use multi-thread version
   * @return
   */
  def zmqDoReduceScatter(spc: SparkleContext,
                         arraySize: Int,
                         parallelism: Option[Int],
                         numAttempts: Int,
                         verySparseMessage: Boolean,
                         seed: Int = 2019): (Int, Double, Double) = {
    val arrayBytes: Long = arraySize.toLong * 8
    val numExecutors = spc.sc.executorLocations.size
    val durations = spc.spawnZmq(comm => {
      {
        // Basically verify the correctness
        val data = Array.fill[Long](arraySize)(comm.rank)
        val expectedValue = 1L * comm.size * (comm.size - 1) / 2
        val reducedData1 = comm.reduceScatterParallelArray[Long](data, _ + _, parallelism.get)
        reducedData1.foreach(v => require(v == expectedValue))
        val reducedData2 = comm.reduceScatterParallelArray[Long](data, _ + _, parallelism.get)
        reducedData2.foreach(v => require(v == expectedValue))
      }
      val rand = new scala.util.Random(seed)
      val data = if (verySparseMessage) {
        Array.fill[Double](arraySize)(seed)
      } else {
        Array.fill[Double](arraySize)(rand.nextDouble())
      }
      if (parallelism.isDefined) {
        comm.reduceScatterParallelArray[Double](data, _ + _, parallelism.get)
      } else {
        require(false)
        // comm.allgather(data)
      }
      val beginTimeMs = System.currentTimeMillis()
      for (i <- 0 until numAttempts) {
        if (parallelism.isDefined) {
          comm.reduceScatterParallelArray[Double](data, _ + _, parallelism.get)
        } else {
          require(false)
          // comm.allgather(data)
        }
      }
      val endTimeMs = System.currentTimeMillis()
      1.0 * (endTimeMs - beginTimeMs) / numAttempts
    })
    val avg_duration_ms = durations.sum / durations.size
    val avg_bw = 2e-3 * arrayBytes.toDouble * (numExecutors - 1).toDouble / numExecutors / avg_duration_ms
    (arraySize, avg_duration_ms, avg_bw)
  }

  /**
   * Measure the allgather throughput of [[ZmqCommunicator]]
   */
  def zmqAllgatherThroughput(spc: SparkleContext, maxParallelism: Int = 4, fromSize: Int = 4, toSize: Int = 512 * 1024, numAttempts: Int = 2): Unit = {
    println("Parallelism Bytes Duration Bandwidth(MBps)")
    var size = fromSize
    while (size <= toSize) {
      for (parallelism <- Array(None) ++ (1 to maxParallelism).map(p => Some(p))) {
        val (sz, duration, bw) = zmqDoAllgather(spc, size, parallelism, numAttempts)
        val ps = parallelism.map(_.toString).getOrElse("None")
        println(s"${ps} ${size} ${duration} ${bw}")
      }
      size *= 2
    }
  }

  def getNumAttempts(numAttempts: Int, size: Int): Int = {
    if (size >= 16 * 1024 * 1024) {
      Math.max(1, numAttempts / 16)
    } else if (size >= 1024 * 1024) {
      Math.max(1, numAttempts / 4)
    } else {
      numAttempts
    }
  }

  /**
   * Measure the ReduceScatter throughput of [[ZmqCommunicator]]
   *
   * @param fromSize          the beginning of size in the number of 8-byte elements (Double)
   * @param toSize            the ending of size in the number of 8-byte elements (Double)
   * @param numAttempts       number of attempts
   *                          Set this number as large as possible for accurate result
   * @param verySparseMessage If this is set true, then the message will be filled with the same number;
   *                          Otherwise, the message will be filled with random number.
   *                          This flag will influence the effect of compression
   */
  def zmqReduceScatterThroughput(spc: SparkleContext,
                                 maxParallelism: Int = 4,
                                 fromSize: Int = 1024,
                                 toSize: Int = 8 * 1024 * 1024,
                                 numAttempts: Int = 2,
                                 verySparseMessage: Boolean = false): Unit = {
    println("Parallelism Bytes Duration Bandwidth(MBps)")
    var size = fromSize
    while (size <= toSize) {
      val currNumAttempts = getNumAttempts(numAttempts, size)
      for (parallelism <- (1 to maxParallelism).map(p => Some(p))) {
        val (sz, duration, bw) = zmqDoReduceScatter(spc, size, parallelism, numAttempts, verySparseMessage)
        val ps = parallelism.map(_.toString).getOrElse("None")
        println(f"REDUCE_SCATTER    ${ps}%4s ${size}% 9d ${1e-3 * duration}%9.6f ${bw}%9.6f")
      }
      size *= 2
    }
  }

  def rddDoAggregateArray(spc: SparkleContext,
                          vectorPerPartition: Int,
                          vectorDimension: Int,
                          numPartition: Int,
                          numTries: Int,
                          maxParallelism: Int): Unit = {
    val dataset = spc.sc.parallelize(0 until (vectorPerPartition * numPartition), numPartition).map(i => {
      val rand = new scala.util.Random(i)
      Array.fill[Long](vectorDimension)(rand.nextLong())
    }).cacheWithStaticScheduling()
    dataset.count()
    var treeNs: Double = 0
    var treeImmNs: Double = 0
    var spagNs: Double = 0
    var sparkleNs = Array.fill[Long](maxParallelism + 1)(0)
    var sparkleComputeNs = Array.fill[Long](maxParallelism + 1)(0)
    var sparkleReductionNs = Array.fill[Long](maxParallelism + 1)(0)
    var sparkleConcatNs = Array.fill[Long](maxParallelism + 1)(0)

    def zeroValue: Array[Long] = Array.fill[Long](vectorDimension)(0)

    def arraySplit(v: Array[Long], i: Int, n: Int): Array[Long] = {
      val bs = v.length / n
      v.slice(i * bs, if (i == n - 1) v.length else (i + 1) * bs)
    }

    def arrayMerge(l: Array[Long], r: Array[Long]): Array[Long] = {
      require(l.length == r.length)
      for (j <- l.indices) {
        l(j) = l(j) + r(j)
      }
      l
    }

    def arrayConcat(ls: IndexedSeq[Array[Long]]): Array[Long] = {
      val len = ls.map(_.length).sum
      val res = new Array[Long](len)
      var offset = 0
      for (tid <- ls.indices) {
        System.arraycopy(ls(tid), 0, res, offset, ls(tid).length)
        offset += ls(tid).length
      }
      res
    }

    {
      val metric = SplitAggregateMetric()
      var treeResult = dataset.treeAggregate(zeroValue)(arrayMerge, arrayMerge)
      var sparkleResult = dataset.splitAggregate(zeroValue)(arrayMerge,
                                                            arrayMerge,
                                                            arraySplit,
                                                            arrayConcat,
                                                            maxParallelism,
                                                            metric)
      require(treeResult.deep == sparkleResult.deep)
    }

    for (i <- 0 until numTries) {
      val t1 = System.nanoTime()
      var treeResult = dataset.treeAggregate(zeroValue)(arrayMerge, arrayMerge)
      treeResult = null
      val t2 = System.nanoTime()
      var treeIMMResult = dataset.treeAggregateWithInMemoryMerge(zeroValue)(arrayMerge, arrayMerge)
      treeIMMResult = null
      val t3 = System.nanoTime()
      //      var spagResult = dataset.spagAggregate(zeroValue)(_ + _,
      //                                                                                       _ + _,
      //                                                                                       SplitOps.denseVectorSplitOpLong,
      //                                                                                       SplitOps.denseVectorConcatOpLong)
      //      spagResult = null
      val t4 = System.nanoTime()
      treeNs += t2 - t1
      treeImmNs += t3 - t2
      spagNs += t4 - t3
      var parallelism = 1
      while (parallelism <= maxParallelism) {
        val bt = System.nanoTime()
        val metric = SplitAggregateMetric()
        var sparkleResult = dataset.splitAggregate(zeroValue)(arrayMerge,
                                                              arrayMerge,
                                                              arraySplit,
                                                              arrayConcat,
                                                              maxParallelism,
                                                              metric)
        sparkleResult = null
        val et = System.nanoTime()
        sparkleNs(parallelism) += et - bt
        sparkleComputeNs(parallelism) += metric.computeTimeNs
        sparkleReductionNs(parallelism) += metric.reductionTimeNs
        sparkleConcatNs(parallelism) += metric.lastConcatTimeNs
        parallelism *= 2
      }
    }
    treeNs /= numTries
    treeImmNs /= numTries
    for (i <- 1 to maxParallelism) {
      sparkleNs(i) /= numTries
      sparkleComputeNs(i) /= numTries
      sparkleReductionNs(i) /= numTries
      sparkleConcatNs(i) /= numTries
    }
    val disp = (1 to maxParallelism).filter(i => sparkleNs(i) > 0).map(i => {
      f"${i}% 3d ${1e-9 * sparkleNs(i)}%9.6f ${1e-9 * sparkleComputeNs(i)}%9.6f ${1e-9 * sparkleReductionNs(i)}%9.6f ${1e-9 * sparkleConcatNs(i)}%9.6f"
    }).mkString(" ")
    spc.sc.unpersistRDD(dataset.id, true)

    println(f"AGGREGATION_ARRAY    ${maxParallelism}% 3d ${numPartition}% 3d ${vectorPerPartition}% 3d ${vectorDimension}% 9d ${1e-9 * treeNs}%9.6f ${1e-9 * treeImmNs}%9.6f ${1e-9 * spagNs}%9.6f ${disp}")
  }

  def rddDoAggregateDenseVector[T](spc: SparkleContext,
                                   vectorPerPartition: Int,
                                   vectorDimension: Int,
                                   numPartition: Int,
                                   numTries: Int,
                                   maxParallelism: Int): Unit = {
    val dataset = spc.sc.parallelize(0 until (vectorPerPartition * numPartition), numPartition).map(i => {
      DenseVector.rand[Long](vectorDimension, Rand.randLong)
    }).cacheWithStaticScheduling()
    def denseVectorSplitOpLong(vec: DenseVector[Long], chunk_idx: Int, num_chunks: Int): DenseVector[Long] = {
      val blockSize = vec.length / num_chunks
      val beginPos = chunk_idx * blockSize
      val endPos = if (chunk_idx == num_chunks - 1) vec.length else (chunk_idx + 1) * blockSize
      vec.slice(beginPos, endPos).copy
    }
    def denseVectorConcatOpLong(vectors: Seq[DenseVector[Long]]): DenseVector[Long] = {
      DenseVector.vertcat(vectors: _*)
    }
    dataset.count()
    var treeNs: Double = 0
    var treeImmNs: Double = 0
    var spagNs: Double = 0
    var sparkleNs = Array.fill[Long](maxParallelism + 1)(0)
    var sparkleComputeNs = Array.fill[Long](maxParallelism + 1)(0)
    var sparkleReductionNs = Array.fill[Long](maxParallelism + 1)(0)
    var sparkleConcatNs = Array.fill[Long](maxParallelism + 1)(0)
    val zeroValue = DenseVector.zeros[Long](vectorDimension)
    for (i <- 0 until numTries) {
      val t1 = System.nanoTime()
      var treeResult = dataset.treeAggregate(zeroValue)(_ + _, _ + _)
      treeResult = null
      val t2 = System.nanoTime()
      var treeIMMResult = dataset.treeAggregateWithInMemoryMerge(zeroValue)(_ + _, _ + _)
      treeIMMResult = null
      val t3 = System.nanoTime()
//      var spagResult = dataset.spagAggregate(zeroValue)(_ + _,
//                                                        _ + _,
//                                                        denseVectorSplitOpLong,
//                                                        denseVectorConcatOpLong)
//      spagResult = null
      val t4 = System.nanoTime()
      treeNs += t2 - t1
      treeImmNs += t3 - t2
      spagNs += t4 - t3
      var parallelism = 1
      while (parallelism <= maxParallelism) {
        val bt = System.nanoTime()
        val metric = SplitAggregateMetric()
        var sparkleResult = dataset.splitAggregate(zeroValue)(_ + _,
                                                              _ + _,
                                                              denseVectorSplitOpLong,
                                                              denseVectorConcatOpLong,
                                                              maxParallelism,
                                                              metric)
        sparkleResult = null
        val et = System.nanoTime()
        sparkleNs(parallelism) += et - bt
        sparkleComputeNs(parallelism) += metric.computeTimeNs
        sparkleReductionNs(parallelism) += metric.reductionTimeNs
        sparkleConcatNs(parallelism) += metric.lastConcatTimeNs
        parallelism *= 2
      }
    }
    treeNs /= numTries
    treeImmNs /= numTries
    for (i <- 1 to maxParallelism) {
      sparkleNs(i) /= numTries
      sparkleComputeNs(i) /= numTries
      sparkleReductionNs(i) /= numTries
      sparkleConcatNs(i) /= numTries
    }
    val disp = (1 to maxParallelism).filter(i => sparkleNs(i) > 0).map(i => {
      f"${i}% 3d ${1e-9 * sparkleNs(i)}%9.6f ${1e-9 * sparkleComputeNs(i)}%9.6f ${1e-9 * sparkleReductionNs(i)}%9.6f ${1e-9 * sparkleConcatNs(i)}%9.6f"
    }).mkString(" ")
    spc.sc.unpersistRDD(dataset.id, true)

    println(f"AGGREGATION_DENSEVECTOR    ${maxParallelism}% 3d ${numPartition}% 3d ${vectorPerPartition}% 3d ${vectorDimension}% 9d ${1e-9 * treeNs}%9.6f ${1e-9 * treeImmNs}%9.6f ${1e-9 * spagNs}%9.6f ${disp}")
  }

  def rddAggregateArray(spc: SparkleContext,
                        vectorPerPartition: Int,
                        numPartition: Int,
                        numTries: Int,
                        maxParallelism: Int,
                        fromDimension: Int = 128,
                        toDimension: Int = 256 * 1024 * 1024): Unit = {
    println("Parallelism NumPartition VectorPerPartition VectorDimension Tree(Sec) Spag(Sec) Sparkle(Sec)")
    var vectorDimension = fromDimension
    while (vectorDimension <= toDimension) {
      rddDoAggregateArray(spc, vectorPerPartition, vectorDimension, numPartition, numTries, maxParallelism)
      vectorDimension *= 2
    }
  }

  def rddAggregateDenseVector(spc: SparkleContext,
                              vectorPerPartition: Int,
                              numPartition: Int,
                              numTries: Int,
                              maxParallelism: Int,
                              fromDimension: Int = 128,
                              toDimension: Int = 256 * 1024 * 1024): Unit = {
    println("Parallelism NumPartition VectorPerPartition VectorDimension Tree(Sec) Spag(Sec) Sparkle(Sec)")
    var vectorDimension = fromDimension
    while (vectorDimension <= toDimension) {
      rddDoAggregateDenseVector(spc, vectorPerPartition, vectorDimension, numPartition, numTries, maxParallelism)
      vectorDimension *= 2
    }
  }
}
