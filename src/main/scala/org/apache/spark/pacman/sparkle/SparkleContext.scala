package org.apache.spark.pacman.sparkle

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockManagerId, ReduceStagePartialResultBlockId, ReducedResultObjectId, StorageLevel}
import org.apache.spark.{SparkContext, SparkEnv, TaskContext}
import org.zeromq.{SocketType, ZMQ}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.Random

case class ZmqInfo(ctx: ZMQ.Context, txAndPorts: IndexedSeq[(ZMQ.Socket, Int)])

case class SplitAggregateMetric(var computeTimeNs: Long = 0, var reductionTimeNs: Long = 0, var lastConcatTimeNs: Long = 0)

/**
 * The context passed to the actors spawned
 */
case class ActorContext(rank: Int, topology: Map[Int, BlockManagerId]) {
  def blockManagerId = topology(rank)
}

class SparkleContext(@transient val sc: SparkContext) extends Serializable {
  import SparkleContext._

  var zmqInit = false

  /**
   * Initialize the ZMQ sockets for each executor. After this call, the ZMQ communicator shall be visible as
   * zmqCommunicatorId in the [[org.apache.spark.storage.LocalObjectManager]]
   *
   * @param numRails       the number of parallel rails to fully utilize the throughput
   * @param numIOThreads   the number of I/O threads of ZMQ context
   * @param useCompression Set true to compress the data the same way as Spark's shuffle data
   */
  def initZMQ(numRails: Int, numIOThreads: Int, useCompression: Boolean = false): Unit = {
    require(numRails >= 1)
    if (zmqInit) {
      throw new RuntimeException("ZeroMQ already initialized")
    }
    zmqInit = true
    val numExecutors = sc.executorLocations.size
    val ranks = sc.parallelize(0 until numExecutors, numExecutors).cacheWithStaticScheduling
    val topology = ranks.map(rank => {
      val objManager = SparkEnv.get.localObjectManager
      require(objManager.get[ZmqInfo](zmqInfoId).isEmpty)
      val ctx = ZMQ.context(numIOThreads)
      val txAndPorts = (0 until numRails).map(_ => {
        val tx = ctx.socket(SocketType.PUSH)
        val port = tx.bindToRandomPort("tcp://*")
        (tx, port)
      })
      objManager.atomicPutOrMutate[ZmqInfo](zmqInfoId, _ => (), ZmqInfo(ctx, txAndPorts))
      (SparkEnv.get.blockManager.blockManagerId.host, txAndPorts.map(_._2), SparkEnv.get.blockManager.blockManagerId.executorId)
    }).collect()
    ranks.map(rank => {
      val prevRank = (rank + numExecutors - 1) % numExecutors
      val prevHost = topology(prevRank)._1
      val prevPorts = topology(prevRank)._2
      val objManager = SparkEnv.get.localObjectManager
      val zi = objManager.get[ZmqInfo](zmqInfoId).get
      val rx_list = prevPorts.map(port => {
        val rx = zi.ctx.socket(SocketType.PULL)
        rx.connect(s"tcp://${prevHost}:${port}")
        rx
      })
      val comm = new ZmqCommunicator(zi.ctx,
                                     rank,
                                     topology,
                                     zi.txAndPorts.map(_._1).zip(rx_list),
                                     SparkEnv.get.serializer,
                                     SparkEnv.get.serializerManager,
                                     useCompression)
      objManager.atomicPutOrMutate[ZmqCommunicator](zmqCommunicatorId, _ => (), comm)
      rank
    }).collect()
  }

  /**
   *
   * @param callback
   * @tparam T
   * @return
   */
  def spawnZmq[T: ClassTag](callback: ZmqCommunicator => T): Array[T] = {
    val numExecutors = sc.executorLocations.size
    sc.parallelize(0 until numExecutors, numExecutors).cacheWithStaticScheduling.map(rank => {
      val objManager = SparkEnv.get.localObjectManager
      val comm = objManager.get[ZmqCommunicator](zmqCommunicatorId).get
      require(comm.rank == rank)
      require(comm.size == numExecutors)
      callback(comm)
    }).collect()
  }

  /**
   * Launch actors
   *
   * @param numExecutors
   * @param callback
   * @tparam T
   * @return
   */
  def spawn[T: ClassTag](numExecutors: Int, callback: ActorContext => T): Array[T] = {
    val totalNumExecutors = sc.executorLocations.size
    assert(numExecutors <= totalNumExecutors)
    val ranks = sc.parallelize(0 until numExecutors, numExecutors).cacheWithStaticScheduling
    val mapping = ranks.map(rank => {
      (rank, SparkEnv.get.blockManager.blockManagerId)
    }).collectAsMap.toMap
    ranks.map(i => callback(ActorContext(i, mapping))).collect()
  }

  def splitAggregate[T: ClassTag, U: ClassTag, V: ClassTag](zeroValue: U)(
    rdd: RDD[T],
    seqOp: (U, T) => U,
    reduceOp: (U, U) => U,
    splitOp: (U, Int, Int) => V,
    subReduceOp: (V, V) => V,
    concatOp: IndexedSeq[V] => V,
    requiredParallelism: Int = 4,
    metricsOpt: Option[SplitAggregateMetric] = None): V = {

    val t1 = System.nanoTime()
    val cleanSeqOp = sc.clean(seqOp)
    val cleanReduceOp = sc.clean(reduceOp)
    val cleanSplitOp = sc.clean(splitOp)
    val cleanSubReduceOp = sc.clean(subReduceOp)
    val cleanConcatOp = sc.clean(concatOp)
    val aggregatePartition = (ctx: TaskContext, it: Iterator[T]) => it.aggregate(zeroValue)(cleanSeqOp, cleanReduceOp)
    val locsWithMetrics = sc.runJobWithReduction[T, U](rdd, aggregatePartition, cleanReduceOp, rdd.partitions.indices)
    val t2 = System.nanoTime()
    val objectIds = locsWithMetrics.values.map(_._1).toSet
    require(objectIds.size == 1)
    val objectId = objectIds.head
    val blockManagerIds = locsWithMetrics.keys.toSet
    require(blockManagerIds == sc.executorLocations.toSet)
    val results = new SpawnRDD[V](sc, sc.executorLocations, comm=>{
      val objectManager = SparkEnv.get.localObjectManager
      val data = objectManager.getAndRemove[U](objectId)
      comm.reduceScatterParallel(data, cleanSplitOp, cleanSubReduceOp, cleanConcatOp, requiredParallelism)
    }).collect()
    val t3 = System.nanoTime()
    val result = concatOp(results)
    val t4 = System.nanoTime()
    metricsOpt match {
      case Some(m) =>
        m.computeTimeNs = t2 - t1
        m.reductionTimeNs = t3 - t2
        m.lastConcatTimeNs = t4 - t3
      case None =>
    }
    result
  }
}

object SparkleContext {
  private var _spc: Option[SparkleContext] = None

  /** The number of I/O threads used by ZMQ */
  var numIOThreads: Int = 4

  /** The number of pairs of connections established */
  var numSockets: Int = 8

  /** Set true to sort the executors by host (improve the performance) */
  var executorSortedByHost: Boolean = true

  /** Set true to enable compression between nodes */
  var compressionEnabled: Boolean = false

  def getOrCreate(sc: SparkContext): SparkleContext = {
    _spc match {
      case None =>
        sc.parallelize(0 until 1024, 1024).count()
        sc.updateExecutorLocations(executorSortedByHost)
        println(s"Number of executors observed = ${sc.executorLocations.size}")
        println("Hosts observed = " + sc.executorLocations.map(_.host).distinct.mkString(" "))
        val spc = new SparkleContext(sc)
        spc.initZMQ(numSockets, numIOThreads, compressionEnabled)
        _spc = Some(spc)
        spc
      case Some(spc) =>
        spc
    }
  }

  val zmqInfoId = ReducedResultObjectId(-1, -1)
  val zmqCommunicatorId = ReducedResultObjectId(-2, -2)
}