package org.apache.spark.pacman.sparkle

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, ReducedResultTask}
import org.apache.spark.storage.{BlockManagerId, ObjectId, ReducedResultObjectId}
import org.apache.spark.util.SpagMetrics

import scala.collection.Map
import scala.reflect.ClassTag

case class ObjectStorePartition(override val index: Int, blockManagerId: BlockManagerId, objectId: ObjectId) extends Partition

/**
 * Create an RDD called ObjectStoreRDD, refer to MapPartitionsRDD, HadoopRDD as an example, override the following:
 *   RDD[U] // hashmap
 *   getPartitions (take no args) // return array of partition contains ExeID and objID
 *   compute //
 *   getPreferredLocations // return location of the parition (ExeID in this case)
 * Check [[ReducedResultTask]] for how to gain access to the object manager
 */
class ObjectStoreRDD[U: ClassTag](sc: SparkContext, locations: IndexedSeq[BlockManagerId], objectId: ObjectId) extends RDD[U](sc, Nil)
{
  override def getPartitions: Array[Partition] = {
    locations.zipWithIndex.map{case (bmId, index) => ObjectStorePartition(index, bmId, objectId)}.toArray
  }

  override def compute(p: Partition, context: TaskContext) : Iterator[U]= {
    val split  = p.asInstanceOf[ObjectStorePartition]
    val objectID = split.objectId
    val result = SparkEnv.get.localObjectManager.getAndRemove[U](objectID)
    Iterator(result)
  }

  override def getPreferredLocations(p: Partition): Seq[String] = {
    val bmid = p.asInstanceOf[ObjectStorePartition].blockManagerId
    val executorCacheTaskLoc = ExecutorCacheTaskLocation(bmid.host, bmid.executorId)
    Array(executorCacheTaskLoc.toString)
  }
}

case class SpawnPartition(override val index: Int, blockManagerId: BlockManagerId) extends Partition

class SpawnRDD[T: ClassTag](sc: SparkContext, locations: IndexedSeq[BlockManagerId], f: ZmqCommunicator=>T) extends RDD[T](sc, Nil)
{
  override def getPartitions: Array[Partition] = {
    locations.zipWithIndex.map{case (bmId, index) => SpawnPartition(index, bmId)}.toArray
  }

  override def compute(p: Partition, context: TaskContext) : Iterator[T]= {
    val split = p.asInstanceOf[ObjectStorePartition]
    val rank = p.index
    val numExecutors = locations.size
    val objManager = SparkEnv.get.localObjectManager
    val comm = objManager.get[ZmqCommunicator](SparkleContext.zmqCommunicatorId).get
    require(comm.rank == rank)
    require(comm.size == numExecutors)
    Iterator(f(comm))
  }

  override def getPreferredLocations(p: Partition): Seq[String] = {
    val bmid = p.asInstanceOf[ObjectStorePartition].blockManagerId
    val executorCacheTaskLoc = ExecutorCacheTaskLocation(bmid.host, bmid.executorId)
    Array(executorCacheTaskLoc.toString)
  }
}