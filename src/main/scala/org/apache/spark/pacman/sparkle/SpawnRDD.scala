package org.apache.spark.pacman.sparkle

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, ReducedResultTask}
import org.apache.spark.storage.{BlockManagerId, ObjectId, ReducedResultObjectId}
import org.apache.spark.util.SpagMetrics

import scala.collection.Map
import scala.reflect.ClassTag

case class SpawnPartition(override val index: Int, blockManagerId: BlockManagerId) extends Partition

/**
 * SpawnRDD refers to a dataset where each partition has an explicitly assigned target executor, its value is created
 * from a user-defined function.
 *
 * @param sc
 * @param locations the target executor (represented by [[BlockManagerId]]) of the partitions
 * @param f the UDF of each partition
 * @tparam T
 */
class SpawnRDD[T: ClassTag](sc: SparkContext, locations: IndexedSeq[BlockManagerId], f: ZmqCommunicator=>T) extends RDD[T](sc, Nil)
{
  override def getPartitions: Array[Partition] = {
    locations.zipWithIndex.map{case (bmId, index) => SpawnPartition(index, bmId)}.toArray
  }

  override def compute(p: Partition, context: TaskContext) : Iterator[T]= {
    val split = p.asInstanceOf[SpawnPartition]
    val rank = p.index
    val numExecutors = locations.size
    val objManager = SparkEnv.get.localObjectManager
    val comm = objManager.get[ZmqCommunicator](SparkleContext.zmqCommunicatorId).get
    require(comm.rank == rank)
    require(comm.size == numExecutors)
    Iterator(f(comm))
  }

  override def getPreferredLocations(p: Partition): Seq[String] = {
    val bmid = p.asInstanceOf[SpawnPartition].blockManagerId
    val executorCacheTaskLoc = ExecutorCacheTaskLocation(bmid.host, bmid.executorId)
    Array(executorCacheTaskLoc.toString)
  }
}