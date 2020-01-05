package org.apache.spark.pacman.cmr_20191124

import scala.collection.mutable

/**** BASIC PROGRAM REPRESENTATION ****/

case class TaskContext(partitionId: Int)

/** Evaluated once */
abstract class Future[T] {
  def wait: Unit
  def get: T
}

trait Anything

/**
 * Value that is split into specific number of chunks
 * @tparam T the type of chunks
 */
trait FixedSplit[T] {
  def numChunks: Int
  def getChunk(i: Int): T
}

/** Represent a value box: its value is either stored or computed */
trait IBox {
  def iget: Either[Anything, Future[Anything]]
}

abstract class Box[T <: Anything] extends IBox {
  override def iget: Either[Anything, Future[Anything]] = ???
  def dependencies: Seq[IBox]
  def get: Either[T, Future[T]]
}

/** A group of parallel value boxes: they are independent to each other */
abstract class PD[T] {
  def size: Int
  def compute(taskContext: TaskContext): T
}

/** Layer for caching the output of dependency */
abstract class CachedPD[T](dep: PD[T]) extends PD[T] {
  val storage: Array[Option[T]] = (0 until dep.size).map(x => None).toArray
  override def compute(taskContext: TaskContext): T = {
    val partitionId = taskContext.partitionId
    if (!storage(partitionId).isDefined) {
      storage(partitionId) = Some(dep.compute(taskContext))
    }
    storage(partitionId).get
  }
  override def size: Int = dep.size
}

/** Layer for transforming the values */
abstract class MapPD[U, T](dep: PD[U]) extends PD[T] {
  def map(in: U): T
  override def compute(taskContext: TaskContext): T = map(dep.compute(taskContext))
  override def size: Int = dep.size
}

/** Layer for zipping the values */
class ZipPD[U1, U2, T](dep1: PD[U1], dep2: PD[U2])(zip: (TaskContext, U1, U2)=>T) extends PD[T] {
  require(dep1.size == dep2.size)
  override def size: Int = dep1.size
  override def compute(taskContext: TaskContext): T = zip(taskContext, dep1.compute(taskContext), dep2.compute(taskContext))
}

/** Layer for exchanging */
class ExchangePD[I<:Serializable, T](dep: PD[FixedSplit[I]])(merge: IndexedSeq[I]=>T) extends PD[T] {
  private def ensurePreviousStage: Unit = ???
  private def getChunk(pid: Int, cid: Int): I = {
    Storage.getLocal[I, FixedSplit[I]](s"shuffle_${pid}", u => u.getChunk(cid))
  }
  override def size: Int = dep.size
  override def compute(taskContext: TaskContext): T = {
    merge((0 until dep.size).map(mapperId => getChunk(mapperId, taskContext.partitionId)))
  }
}

/** Manager for execution */
object Executor {
}

/** Manager for storage */
object Storage {
  def put(k: String, v: Anything): Unit = ???
  def getLocal[I, T](k: String, c: T=>I): I = ???
  def getGlobal[I <: Serializable, T](k: String, c: T=>I): Future[I] = ???
}

class VertexOffset

class VertexId {
  def pid: Int = ???
  def offset: VertexOffset = ???
}

object VertexId {
  def fromPartitionOffset(pid: Int, offset: VertexOffset): VertexId = ???
}

class LSMT[T] {
  def batchInsert[U](r: Seq[U])(s: U=>T): Unit = ???
  def batchFilterExist[U](r: Seq[U])(s: U=>T): Seq[U] = ???
  def batchFilterNotExist[U](r: Seq[U])(s: U=>T): Seq[U] = ???
}

class KVS[K, V] {
  def get(key: K): V = ???
}

object BFS {
  case class BfsContrib(vid: VertexId, parent: VertexId)
  def unique(sorted: Array[BfsContrib]): Array[BfsContrib] = ???
  def parted(sorted: Array[BfsContrib]): FixedSplit[Array[BfsContrib]] = ???
  def gather[T](arrs: Seq[Array[T]]): Array[T]
  def main(args: Array[String]): Unit = {
    val csrGraph: PD[KVS[VertexOffset, Array[VertexId]]] = ???
    val initFrontier: PD[Array[VertexOffset]] = ???
    var visitedPLS: PD[LSMT[VertexOffset]] = ???
    val contrib = new ZipPD(csrGraph, initFrontier)((taskContext, csr, frontier)=> {
      val pid = taskContext.partitionId
      val builder = mutable.ArrayBuilder.make[BfsContrib]()
      frontier.foreach(v => {
        csr.get(v).foreach(dst => builder += BfsContrib(dst, VertexId.fromPartitionOffset(pid, v)))
      })
      parted(unique(builder.result().sortBy(c => (c.vid, c.parent))))
    })
    val contribPLS = new ExchangePD(contrib)(inRaw => {
      unique(gather(inRaw).sortBy(c => (c.vid, c.parent)))
    })
    /**
     * Because visitedPLS is mutable, several constraint must be added
     *   1. nextFrontier can only be evaluated once. If so, re-computation-based fault tolerance is impossible
     * However, there are other ways so that fault tolerance is preserved:
     *   1. Because the mutation is based on batch update, it seems that we can use re-computation-based fault tolerance.
     */
    val nextFrontier = new ZipPD(contribPLS, visitedPLS)((taskContext, c, v)=>{
      val rst = v.batchFilterNotExist(c)(u => u.vid.offset).toArray
      v.batchInsert(c)(u => u.vid.offset)
      rst
    })
  }
}