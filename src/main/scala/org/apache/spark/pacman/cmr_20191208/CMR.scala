package org.apache.spark.pacman.cmr_20191208

class Future[T]

trait Istream
trait Ostream

trait Buffer

trait IoQueue

/** Object is locally stored, i.e., not distributed */
trait Object

/** Serializable object can be serialized into byte stream */
trait Serializable extends Object

/** Provides a unified abstraction for various kinds of file systems */
trait StorageBackend {
  /** Read specified range from given path into a buffer */
  def read(path: String, offset: Long, size: Long): Future[Buffer]

  /** Write the buffer into specified range from given path */
  def write(path: String, offset: Long, buf: Buffer): Future[Unit]

  /** Remove the file */
  def unlink(path: String): Unit
}

// TODO: Can we unify read and deserialize?
// We need deserialize because it is required by merging

/** Thread pool */
class ExecutionContext

class ExecutionPolicy
case class ParallelExecutionPolicy(ec: ExecutionContext, parallelism: Int)

trait ObjectId {
  def name: String
  def familyId: Int
  def familyOffset: Int
}
/** Read-write */
class CachedPartitionObjectId(opId: Int, partitionId: Int) extends ObjectId {
  override def name: String = s"cache_${opId}_${partitionId}"
  override def familyId: Int = opId
  override def familyOffset: Int = partitionId
}
/** Write-only: offer data for shuffle reduce */
class ShuffleWriteObjectId(opId: Int, mapTaskId: Int) extends ObjectId {
  override def name: String = s"shufflemap_${opId}_${mapTaskId}"
  override def familyId: Int = opId
  override def familyOffset: Int = mapTaskId
}
/** Read-only: available when all the corresponding shuffle map is ready */
class ShuffleReadObjectId(opId: Int, reduceTaskId: Int) extends ObjectId {
  override def name: String = s"shufflereduce_${opId}_${reduceTaskId}"
  override def familyId: Int = opId
  override def familyOffset: Int = reduceTaskId
}

/**
 * A swapper knows how to read/write an object from/into the storage backend.
 * [Note] The read/write may be parallel
 */
trait Swapper {
  /** Parallel-read an object from blob store */
  def read(storageBackend: StorageBackend, objectId: ObjectId, policy: ExecutionPolicy): Future[Object]

  /** Parallel-write an object to blob store */
  def write(obj: Object, storageBackend: StorageBackend, objectId: ObjectId, policy: ExecutionPolicy): Future[Unit]

  /**
   * Remove the object from blob store:
   * [Garuantee] No pending read/write before this call
   */
  def remove(storageBackend: StorageBackend, objectId: ObjectId, policy: ExecutionPolicy): Unit
}

/**
 * A general swapper for objects that consist of only buffers
 */
abstract class ZeroCopySwapper extends Swapper {
  /** Number of buffers the object has */
  def numBuffers(obj: Object): Int

  /** Get the buffers from the object */
  def intoBuffers(obj: Object): Seq[Buffer]

  /** Assemble the object from buffers */
  def fromBuffers(bufs: Seq[Buffer]): Object

  override def read(storageBackend: StorageBackend, objectId: ObjectId, policy: ExecutionPolicy): Future[Object] = ???

  override def write(obj: Object, storageBackend: StorageBackend, objectId: ObjectId, policy: ExecutionPolicy): Future[Unit] = ???

  override def remove(storageBackend: StorageBackend, objectId: ObjectId, policy: ExecutionPolicy): Unit = ???
}

trait FamilyId {
  def name: String
}
trait AccessPatternHint
class ParOrderedOnce extends AccessPatternHint
case class ParOrderedMultiple(times: Int) extends AccessPatternHint
case class ParOrderedMultipleInMemory(times: Int) extends AccessPatternHint

/** An object family is a group of objects */
class ObjectFamily(val numObjects: Int,
                   val accessPatternHint: AccessPatternHint,
                   val evaluator: Int=>Object,
                   val mutationOp: (Object, Object)=>Object,
                   val swapper: Swapper) {}

/**
 * The object store is responsible for storing objects
 * It is responsible for handle prefetching and hide the I/O and computation overlapping from the user
 */
class ObjectStore {
  /** Register the family to the object store */
  def registerFamily(familyId: FamilyId, numObjects: Int, accessPatternHint: AccessPatternHint, swapper: Swapper): Unit = ???

  /** Remove the object family */
  def removeFamily(familyId: FamilyId): Unit = ???

  /** Put the object into the object store */
  def put(objectId: ObjectId, obj: Object, policy: ExecutionPolicy): Unit = ???

  /** Get the object from the object store */
  def get(objectId: ObjectId, policy: ExecutionPolicy): Object = ???
}

/** A serdes knows how to serialize and deserialize */
trait Serdes {
  /** Deserialize sequentially from byte stream */
  def deserialize(is: Istream): Object

  /** Serialize sequentially into byte stream */
  def serialize(os: Ostream): Unit
}

/**
 * Serializable array is an array of serializables, can be used for storing intermediate results for data exchanging
 */
abstract class SerializableArray[T <: Object] extends Object {
  def size: Int

  def at(i: Int): T
}

/**
 * A reader that also knows how to read an object from the Array[Object] stored in the file system  (may be parallel)
 * In addition:
 *   1) Random access, which is provided by `readaheadAt` and `readahead`.
 */
class SerializableArrayReaderWriter extends Swapper {
  def size: Int = ???

  def readaheadAt(pos: Int, path: String, fileSystem: StorageBackend, parallelism: Int = 1): Unit = ???

  def readAt(pos: Int, path: String, fileSystem: StorageBackend, parallelism: Int = 1): Object = ???

  override def readahead(path: String, fileSystem: StorageBackend, parallelism: Int): Unit = ???

  override def read(path: String, fileSystem: StorageBackend, parallelism: Int): Object = ???

  /**
   * Trade-off between meta-data overheads and I/O parallelism
   *
   * Several versions. Denote the number of workers W, ideal parallelism P_ideal, ideal parallelism per worker P=P_ideal/W, the number of mappers M, the number of reducers R
   *   1. Merge from R to P files when R > P: requires serialize/deserialize semantic of T rather than Reader/Writer to merge in memory
   *   2. Split from R to P files when R < P: Reader/Writer is OK
   *
   * @param obj
   * @param path
   * @param fileSystem
   * @param parallelism
   */
  override def write(obj: Object, path: String, fileSystem: StorageBackend, parallelism: Int): Unit = ???
}

class SerializableArraySerdes extends Serdes {
  override def deserialize(is: Istream): Object = ???

  override def serialize(os: Ostream): Unit = ???
}

/**
 * Represent a distributed dataset of type T
 *
 * Distributed[T] is similar to Array[T].
 * Distributed[T] represents a array of lazily-evaluated objects.
 * Each element in Distributed[T] can be independently computed.
 */
abstract class Distributed[T <: Object]

class CmrContext {
  //////////////////////////////// Operations /////////////////////////////////////////////

  /** N -> N */
  def map[U <: Object, V <: Object](u: Distributed[U], mapOp: U => V): Distributed[V] = ???

  /** (N, N) -> N */
  def zip[U1 <: Object, U2 <: Object](u1: Distributed[U1], u2: Distributed[U2]): Distributed[(U1, U2)] = ???

  /** (M, N) -> M*N */
  def crossProduct[U1 <: Object, U2 <: Object](u1: Distributed[U1], u2: Distributed[U2]): Distributed[(U1, U2)] = ???

  /** N -> N */
  def persist[U <: Object](u: Distributed[U], eager: Boolean): Distributed[U] = ???

  /** M -> N */
  def exchange[U <: Serializable](dd: Distributed[SerializableArray[U]]): Distributed[SerializableArray[U]] = ???

  //////////////////////////////// Actions /////////////////////////////////////////////

  /** For each elements in the distributed */
  def foreach[T <: Object](t: Distributed[T], op: T => Unit): Unit = ???
}