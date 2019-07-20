package org.apache.spark.pacman.sparkle

import org.apache.spark.SparkEnv
import scala.util.Random

/**
 * Benchmarks
 */
object Benchmark {
  /**
   * Measures the performance of [[org.apache.spark.storage.BlockBasedMessagePassingManager]]
   * @param spc
   * @param nrt the number of round-trips
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
   * @param spc
   */
  def bmMeasureLatency(spc: SparkleContext): Unit = {
    var size = 8
    val nrt = 128
    val maxSize = 1024
    while (size <= maxSize) {
      val rtt = bmPingpong(spc, nrt, size/8)
      val latency = rtt/2
      val mbps = 1e-6*size/latency
      println(s"${size} bytes   ${1e6*latency} us   ${mbps} MB/s")
      size *= 2
    }
  }

  /**
   * Perform allgather with random data and measure the time
   * @param parallelism The parallelism of allgather
   *                      None: use single-thread allgather
   *                      Some(p): use multi-thread allgather
   * @return
   */
  def zmqDoAllgather(spc: SparkleContext, arrayBytes: Int, parallelism: Option[Int], numAttempts: Int = 2, seed: Int = 2019): (Int, Double, Double) = {
    val numExecutors = spc.sc.executorLocations.size
    val durations = spc.spawnZmq(comm=>{
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
      1.0*(endTimeMs - beginTimeMs)/numAttempts
    })
    val avg_duration_ms = durations.sum / durations.size
    val avg_bw = 1e-3 * arrayBytes / (1.0*avg_duration_ms/(2*(numExecutors-1)))
    (arrayBytes, avg_duration_ms, avg_bw)
  }

  /**
   * Perform ReduceScatter with random data and measure the time
   * @param parallelism The parallelism
   *                      None: use single-thread version
   *                      Some(p): use multi-thread version
   * @return
   */
  def zmqDoReduceScatter(spc: SparkleContext, arrayBytes: Int, parallelism: Option[Int], numAttempts: Int = 2, seed: Int = 2019): (Int, Double, Double) = {
    require(arrayBytes % 8 == 0)
    val numExecutors = spc.sc.executorLocations.size
    val durations = spc.spawnZmq(comm=>{
      {
        // Basically verify the correctness
        val data = Array.fill[Long](arrayBytes/8)(comm.rank)
        val expectedValue = 1L * comm.size * (comm.size-1) / 2
        val reducedData1 = comm.reduceScatterParallel[Long](data, _+_, parallelism.get)
        reducedData1.foreach(v => require(v == expectedValue))
        val reducedData2 = comm.reduceScatterParallel[Long](data, _+_, parallelism.get)
        reducedData2.foreach(v => require(v == expectedValue))
      }
      val rand = new scala.util.Random(seed)
      val data = Array.fill[Double](arrayBytes/8)(rand.nextDouble())
      if (parallelism.isDefined) {
        comm.reduceScatterParallel[Double](data, _+_, parallelism.get)
      } else {
        require(false)
        // comm.allgather(data)
      }
      val beginTimeMs = System.currentTimeMillis()
      for (i <- 0 until numAttempts) {
        if (parallelism.isDefined) {
          comm.reduceScatterParallel[Double](data, _+_, parallelism.get)
        } else {
          require(false)
          // comm.allgather(data)
        }
      }
      val endTimeMs = System.currentTimeMillis()
      1.0*(endTimeMs - beginTimeMs)/numAttempts
    })
    val avg_duration_ms = durations.sum / durations.size
    val avg_bw = 2e-3 * arrayBytes * (numExecutors-1).toDouble / numExecutors / avg_duration_ms
    (arrayBytes, avg_duration_ms, avg_bw)
  }

  /**
   * Measure the allgather throughput of [[ZmqCommunicator]]
   */
  def zmqAllgatherThroughput(spc: SparkleContext, maxParallelism: Int = 4, fromSize: Int = 4, toSize: Int = 512*1024, numAttempts: Int = 2): Unit = {
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

  /**
   * Measure the ReduceScatter throughput of [[ZmqCommunicator]]
   */
  def zmqReduceScatterThroughput(spc: SparkleContext, maxParallelism: Int = 4, fromSize: Int = 1024, toSize: Int = 8*1024*1024, numAttempts: Int = 2): Unit = {
    println("Parallelism Bytes Duration Bandwidth(MBps)")
    var size = fromSize
    while (size <= toSize) {
      for (parallelism <- (1 to maxParallelism).map(p => Some(p))) {
        val (sz, duration, bw) = zmqDoReduceScatter(spc, size, parallelism, numAttempts)
        val ps = parallelism.map(_.toString).getOrElse("None")
        println(s"${ps} ${size} ${duration} ${bw}")
      }
      size *= 2
    }
  }
}
