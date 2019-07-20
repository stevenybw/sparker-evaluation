package org.apache.spark.pacman.sparkle

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockManagerId, ReducedResultObjectId, StorageLevel}
import org.apache.spark.{SparkContext, SparkEnv}
import org.zeromq.{SocketType, ZMQ}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.Random

case class ZmqInfo(ctx: ZMQ.Context, txAndPorts: IndexedSeq[(ZMQ.Socket, Int)])

/**
 * The context passed to the actors spawned
 */
case class ActorContext(rank: Int, topology: Map[Int, BlockManagerId]) {
  def blockManagerId = topology(rank)
}

class SparkleContext(@transient val sc: SparkContext) extends Serializable {
  var zmqInit = false
  private val zmqInfoId = ReducedResultObjectId(-1, -1)
  private val zmqCommunicatorId = ReducedResultObjectId(-2, -2)

  /**
   * Initialize the ZMQ sockets for each executor. After this call, the ZMQ communicator shall be visible as
   * zmqCommunicatorId in the [[org.apache.spark.storage.LocalObjectManager]]
   * @param numRails the number of parallel rails to fully utilize the throughput
   * @param useCompression Set true to compress the data the same way as Spark's shuffle data
   */
  def initZMQ(numRails: Int, useCompression: Boolean = false): Unit = {
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
      val ctx = ZMQ.context(4)
      val txAndPorts = (0 until numRails).map(_ => {
        val tx = ctx.socket(SocketType.PUSH)
        val port = tx.bindToRandomPort("tcp://*")
        (tx, port)
      })
      objManager.atomicPutOrMutate[ZmqInfo](zmqInfoId, _=>(), ZmqInfo(ctx, txAndPorts))
      (SparkEnv.get.blockManager.blockManagerId.host, txAndPorts.map(_._2), SparkEnv.get.blockManager.blockManagerId.executorId)
    }).collect()
    ranks.map(rank => {
      val prevRank = (rank+numExecutors-1)%numExecutors
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
      objManager.atomicPutOrMutate[ZmqCommunicator](zmqCommunicatorId, _=>(), comm)
      rank
    }).collect()
  }

  /**
   *
   * @param callback
   * @tparam T
   * @return
   */
  def spawnZmq[T: ClassTag](callback: ZmqCommunicator=>T): Array[T] = {
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
   * @param numExecutors
   * @param callback
   * @tparam T
   * @return
   */
  def spawn[T: ClassTag](numExecutors: Int, callback: ActorContext=>T): Array[T] = {
    val totalNumExecutors = sc.executorLocations.size
    assert(numExecutors <= totalNumExecutors)
    val ranks = sc.parallelize(0 until numExecutors, numExecutors).cacheWithStaticScheduling
    val mapping = ranks.map(rank => {
      (rank, SparkEnv.get.blockManager.blockManagerId)
    }).collectAsMap.toMap
    ranks.map(i => callback(ActorContext(i, mapping))).collect()
  }
}

object SparkleContext {
}