/**
 * A brief introduction to CMR's partition-centric interface
 */

package org.apache.spark.pacman.cmr

import java.io.{InputStream, OutputStream}

trait HeapType
trait FlatType
trait CsrType
class WorkerInfo
class PartitionInfo

/** A slice to an array of T */
class Span[T]

/** Equivalent to std::vector<T> */
class Vector[T] {
  def borrow: Span[T] = ???
}

case class TaskContext(workerInfo: WorkerInfo, partitionInfo: PartitionInfo)

case class FileSpan(path: String, offset: Long, size: Long)

/** Block: type-erased partition */
abstract class Block
abstract class ChunkedBlock

// Data model

//// 1. Partition

/** Partition: anything that stores an array of T */
abstract class Partition[T] extends Block {
  def borrow: Span[T]
}

/** HeapPartition: when T = String, Vector, etc */
class HeapPartition[T <: HeapType] extends Partition[T] {
  val data: Vector[T] = new Vector[T]
  override def borrow: Span[T] = data.borrow
}

/** FlatPartition: when T = Int, Double, Float, etc */
class FlatPartition[T <: FlatType] extends Partition[T] {
  val data: Vector[T] = new Vector[T]
  override def borrow: Span[T] = data.borrow
}

trait CsrConverter[T <: CsrType] {
  type FixedType

  def selectFix(t: T): FixedType
  def selectVar(t: T): Span[Byte]
  def reconstruct(f: FixedType, v: Span[Byte]): T
}

/** CsrPartition: when T = StringView, Span, etc */
class CsrPartition[T <: CsrType, C <: FlatType](cvt: CsrConverter[T]) extends Partition[T] {
  val data: Vector[T] = new Vector[T]
  val pool: Vector[C] = new Vector[C]
  override def borrow: Span[T] = data.borrow
}

//// 2. ChunkedPartition

class ChunkedPartition[T](partition: Partition[T], index: IndexedSeq[Long]) extends ChunkedBlock {
  def numChunks: Int = ???
  def borrowChunk(cid: Int): Span[T] = ???
}

// IO

abstract class BlockSerializer {
  def beforeStore(name: String, block: Block): FileSpan
  def serialize(os: OutputStream, block: Block): Unit
  def beforeRead(name: String): FileSpan
  def deserialize(is: InputStream): Block
}

abstract class ChunkedBlockSerializer extends BlockSerializer {
  def beforeSlice(name: String): FileSpan
  def deserializeSlice(is: InputStream): ChunkedBlock
}

abstract class BlockSplitter {
  def split(block: Block, splitId: Int, numSplits: Int): Block
  def alloc(splits: IndexedSeq[Block]): Block
  def merge(destination: Block, split: Block, splitId: Int, numSplits: Int): Unit
}

//// 0. Heap partition

class HeapPartitionSerializer[T <: HeapType] extends BlockSerializer {
  override def beforeStore(name: String, block: Block): FileSpan = ???
  override def serialize(os: OutputStream, block: Block): Unit = ???
  override def beforeRead(name: String): FileSpan = ???
  override def deserialize(is: InputStream): Block = ???
}

class HeapPartitionSplitter[T <: HeapType] extends BlockSplitter {
  override def split(block: Block, splitId: Int, numSplits: Int): Block = ???
  override def alloc(splits: IndexedSeq[Block]): Block = ???
  override def merge(destination: Block, split: Block, splitId: Int, numSplits: Int): Unit = ???
}

//// 1. Flat partition

class FlatPartitionSerializer[T <: FlatType] extends BlockSerializer {
  override def beforeStore(name: String, block: Block): FileSpan = ???
  override def serialize(os: OutputStream, block: Block): Unit = ???
  override def beforeRead(name: String): FileSpan = ???
  override def deserialize(is: InputStream): Block = ???
}

class FlatPartitionSplitter[T <: FlatType] extends BlockSplitter {
  override def split(block: Block, splitId: Int, numSplits: Int): Block = ???
  override def alloc(splits: IndexedSeq[Block]): Block = ???
  override def merge(destination: Block, split: Block, splitId: Int, numSplits: Int): Unit = ???
}

//// 2. Csr partition

class CsrPartitionSerializer[T <: CsrType] extends BlockSerializer {
  override def beforeStore(name: String, block: Block): FileSpan = ???
  override def serialize(os: OutputStream, block: Block): Unit = ???
  override def beforeRead(name: String): FileSpan = ???
  override def deserialize(is: InputStream): Block = ???
}

class CsrPartitionSplitter[T <: CsrType] extends  BlockSplitter {
  override def split(block: Block, splitId: Int, numSplits: Int): Block = ???
  override def alloc(splits: IndexedSeq[Block]): Block = ???
  override def merge(destination: Block, split: Block, splitId: Int, numSplits: Int): Unit = ???
}

//// 3. Chunked partition

class ChunkedPartitionSerializer[T] extends ChunkedBlockSerializer {
  override def beforeStore(name: String, block: Block): FileSpan = ???
  override def serialize(os: OutputStream, block: Block): Unit = ???
  override def beforeRead(name: String): FileSpan = ???
  override def deserialize(is: InputStream): Block = ???
  override def beforeSlice(name: String): FileSpan = ???
  override def deserializeSlice(is: InputStream): ChunkedBlock = ???
}

// Computation

abstract class Operation[T] {
  def beforeCompute(taskContext: TaskContext): Unit
  def compute(taskContext: TaskContext): Partition[T]
  def beforeGetOrCompute(taskContext: TaskContext): Unit = ???
  def getOrCompute(taskContext: TaskContext): Partition[T] = ???
}

abstract class MapPartition[U, V] extends Operation[V] {
  def mapPartition(taskContext: TaskContext, in: Partition[U]): Partition[V]
  override def beforeCompute(taskContext: TaskContext): Unit = ???
  override def compute(taskContext: TaskContext): Partition[V] = ???
}

abstract class ZipPartition[U1, U2, V] extends Operation[V] {
  def zipPartitions(taskContext: TaskContext, in1: Partition[U1], in2: Partition[U2]): Partition[V]
  override def beforeCompute(taskContext: TaskContext): Unit = ???
  override def compute(taskContext: TaskContext): Partition[V] = ???
}

abstract class Exchange[I, V] extends Operation[V] {
  def beforeComputeMap(taskContext: TaskContext): Unit
  def computeExchangeMap(taskContext: TaskContext): ChunkedPartition[I]
  def computeExchangeReduce(taskContext: TaskContext, in: IndexedSeq[Span[I]]): Partition[V]
  override def beforeCompute(taskContext: TaskContext): Unit = ???
  override def compute(taskContext: TaskContext): Partition[V] = ???
}

abstract class MapExchange[U, I, V] extends Exchange[I, V] {
  def mapPartition(taskContext: TaskContext, in: Partition[U]): ChunkedPartition[I]
  override def beforeComputeMap(taskContext: TaskContext): Unit = ???
  override def computeExchangeMap(taskContext: TaskContext): ChunkedPartition[I] = ???
  override def computeExchangeReduce(taskContext: TaskContext, in: IndexedSeq[Span[I]]): Partition[V] = ???
}

abstract class ZipExchange[U1, U2, I, V] extends Exchange[I, V] {
  def zipPartitions(taskContext: TaskContext, in1: Partition[U1], in2: Partition[U2]): ChunkedPartition[I]
  override def beforeComputeMap(taskContext: TaskContext): Unit = ???
  override def computeExchangeMap(taskContext: TaskContext): ChunkedPartition[I] = ???
  override def computeExchangeReduce(taskContext: TaskContext, in: IndexedSeq[Span[I]]): Partition[V] = ???
}

abstract class Zip3Exchange[U1, U2, U3, I, V] extends Exchange[I, V] {
  def zip3Partitions(taskContext: TaskContext, in1: Partition[U1], in2: Partition[U2], in3: Partition[U3]): ChunkedPartition[I]
  override def beforeComputeMap(taskContext: TaskContext): Unit = ???
  override def computeExchangeMap(taskContext: TaskContext): ChunkedPartition[I] = ???
  override def computeExchangeReduce(taskContext: TaskContext, in: IndexedSeq[Span[I]]): Partition[V] = ???
}
