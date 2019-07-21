package org.apache.spark.pacman.sparkle

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.Executors

import net.jpountz.lz4.{LZ4BlockOutputStream, LZ4Factory}
import net.jpountz.xxhash.XXHashFactory
import org.apache.spark.SparkEnv
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance, SerializerManager}
import org.apache.spark.storage.ShuffleBlockId
import org.zeromq.ZMQ

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.reflect.ClassTag

/**
 *
 * The communicator
 *
 * Currently only ring-based topology has been implemented
 *
 * @param ctx             The ZeroMQ context
 * @param _serializer     Spark serializer
 * @param _serManager     Spark serializer manager
 * @param _useCompression Set true to use compression (the same way as Spark's shuffle data)
 */
class ZmqCommunicator(val ctx: ZMQ.Context,
                      val rank: Int,
                      val topology: IndexedSeq[(String, IndexedSeq[Int], String)],
                      txAndRx: IndexedSeq[(ZMQ.Socket, ZMQ.Socket)],
                      _serializer: Serializer,
                      _serManager: SerializerManager,
                      _useCompression: Boolean) {

  /** ByteArrayOutputStream with its buffer exposed */
  class MyByteArrayOutputStream(size: Int) extends ByteArrayOutputStream(size) {
    def getBuf: Array[Byte] = buf
  }

  /** ByteArrayInputStream with its buffer exposed */
  class MyByteArrayInputStream extends ByteArrayInputStream(Array()) {
    def setBuf(b: Array[Byte]): Unit = {
      buf = b
      pos = 0
      count = b.length
      mark = 0
    }
  }

  /**
   * Thread-local context
   *
   * Provide object-level communication.
   *
   * @param serInstance
   * @param serBuffer
   * @param serOutputStream
   * @param desBuffer
   * @param desInputStream
   */
  case class _TLC(serInstance: SerializerInstance,
                  serBuffer: MyByteArrayOutputStream,
                  wos: OutputStream,
                  serOutputStream: SerializationStream,
                  desBuffer: MyByteArrayInputStream,
                  wis: InputStream,
                  desInputStream: DeserializationStream,
                  defaultCompression: Boolean) {
    def sendObjectWithCompression[T: ClassTag](tx: ZMQ.Socket, t: T): Unit = {
      serBuffer.reset()
      serOutputStream.writeObject(t)
      serOutputStream.flush()
      wos.flush()
      tx.send(serBuffer.getBuf, 0, serBuffer.size(), 0)
    }

    def recvObjectWithCompression[T: ClassTag](rx: ZMQ.Socket): T = {
      desBuffer.setBuf(rx.recv())
      desInputStream.readObject[T]()
    }

    def compressObject[T: ClassTag](t: T): Array[Byte] = {
      serBuffer.reset()
      serOutputStream.writeObject(t)
      serOutputStream.flush()
      wos.flush()
      serBuffer.getBuf.slice(0, serBuffer.size())
    }

    def decompressObject[T: ClassTag](b: Array[Byte]): T = {
      desBuffer.setBuf(b)
      desInputStream.readObject[T]()
    }

    def sendObjectDirect[T: ClassTag](tx: ZMQ.Socket, t: T): Unit = {
      tx.send(serInstance.serialize(t).array())
    }

    def recvObjectDirect[T: ClassTag](rx: ZMQ.Socket): T = {
      serInstance.deserialize[T](ByteBuffer.wrap(rx.recv()))
    }

    def sendObject[T: ClassTag](tx: ZMQ.Socket, t: T): Unit = {
      if (defaultCompression) {
        sendObjectWithCompression(tx, t)
      } else {
        sendObjectDirect(tx, t)
      }
    }

    def recvObject[T: ClassTag](rx: ZMQ.Socket): T = {
      if (defaultCompression) {
        recvObjectWithCompression[T](rx)
      } else {
        recvObjectDirect[T](rx)
      }
    }
  }

  private val _tlc = new ThreadLocal[_TLC] {
    override def initialValue(): _TLC = {
      val DEFAULT_SEED = 0x9747b28c
      val serInstance = _serializer.newInstance()
      val serBuffer = new MyByteArrayOutputStream(1024 * 1024)
      // It is presumed that compression will be used
      val blockSize = SparkEnv.get.conf.getSizeAsBytes("spark.io.compression.lz4.blockSize", "32k").toInt
      val wos = new LZ4BlockOutputStream(serBuffer, blockSize, LZ4Factory.fastestInstance().fastCompressor(), XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), true)
      val serOutputStream = serInstance.serializeStream(wos)
      val desBuffer = new MyByteArrayInputStream
      val wis = _serManager.wrapStream(ShuffleBlockId(-1, -1, -1), desBuffer)
      val desInputStream = serInstance.deserializeStream(wis)
      _TLC(serInstance, serBuffer, wos, serOutputStream, desBuffer, wis, desInputStream, _useCompression)
    }
  }

  private val _parallelism = txAndRx.size

  topology.foreach(e => require(e._2.size == _parallelism))

  /** The degree of parallelism of the communicator */
  def parallelism: Int = _parallelism

  private implicit val _tp: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))

  /** The size of the communicator */
  def size: Int = topology.size

  /** Get thread-local context */
  def tlc: ThreadLocal[_TLC] = _tlc

  /** The serializer for current communicator */
  // def serializer: SerializerInstance = _si

  /** Get the sender socket */
  def getTx(id: Int): ZMQ.Socket = txAndRx(id)._1

  /** Get the receiver socket */
  def getRx(id: Int): ZMQ.Socket = txAndRx(id)._2

  /**
   * Allgather with only one thread
   *
   * @param data the input data
   * @return
   */
  def allgather(data: Array[Byte]): Array[Array[Byte]] = {
    val tx = getTx(0)
    val rx = getRx(0)
    val result = new Array[Array[Byte]](size)
    result(rank) = data
    tx.send(data, 0)
    for (i <- 1 until size - 1) {
      val received = rx.recv(0)
      result((rank + size - i) % size) = received
      tx.send(received, 0)
    }
    val received = rx.recv(0)
    result((rank + 1) % size) = received
    result.foreach(e => require(e != null))
    result
  }

  /**
   * ReduceScatter with only one thread
   *
   * @param data     the input data for each destination
   * @param reduceOp how the two element can be reduced
   * @tparam T
   * @return a single element that has been reduced
   */
  def reduceScatter[T: ClassTag](data: IndexedSeq[T], reduceOp: (T, T) => T): T = {
    val tlc = _tlc.get()
    require(data.size == size)
    val tx = getTx(0)
    val rx = getRx(0)
    tlc.sendObject(tx, data((rank + size - 1) % size))
    for (i <- 2 until size) {
      val received = tlc.recvObject[T](rx)
      val toSend = reduceOp(data((rank + size - i) % size), received)
      tlc.sendObject(tx, toSend)
    }
    val received = tlc.recvObject[T](rx)
    reduceOp(data(rank), received)
  }

  /**
   * ReduceScatter on array type
   *
   * @param data     the input data in the form of array
   *                 Every participant's data must be in the same dimension
   * @param reduceOp how two element can be reduced
   * @tparam T
   * @return
   */
  def reduceScatterParallelArray[T: ClassTag](data: Array[T], reduceOp: (T, T) => T, requiredParallelism: Int = parallelism): Array[T] = {
    reduceScatterParallel[Array[T], Array[T]](
      data,
      (v, i, n) => {
        val bs = v.length / n
        data.slice(i * bs, if (i == n - 1) v.length else (i + 1) * bs)
      },
      (l, r) => {
        require(l.length == r.length)
        for (j <- l.indices) {
          l(j) = reduceOp(l(j), r(j))
        }
        l
      },
      ls => {
        val len = ls.map(_.length).sum
        val res = new Array[T](len)
        // Parallel concatenate does not improve the performance
        //        val pfx = ls.map(_.length).scanLeft(0)(_ + _)
        //        val futures = ls.indices.map(tid => {
        //          Future {
        //                   require(pfx(tid + 1) - pfx(tid) == ls(tid).length)
        //                   System.arraycopy(ls(tid), 0, res, pfx(tid), ls(tid).length)
        //                 }
        //        })
        //        Await.result(Future.sequence(futures), Duration.Inf)
        var offset = 0
        for (tid <- ls.indices) {
          System.arraycopy(ls(tid), 0, res, offset, ls(tid).length)
          offset += ls(tid).length
        }
        res
      },
      requiredParallelism)
  }

  /**
   * General communication routine providing reduce-scatter capability. To fully utilize the computing power inside an
   * executor, multiple threads will be employed.
   *
   * @param data                the input combiner
   * @param splitOp             defines how to split a combiner into a sequence of combiners
   *                            Prototype: (input combiner, block index, number of blocks) => split combiner
   * @param reduceOp            defines how to merge two combiners within the same segment
   *                            Prototype: (combiner 1, combiner 2) => merged combiner
   * @param concatOp            defines how to concatenate a sequence of combiners into a single combiner
   * @param requiredParallelism the number of threads that will be employed in the communication
   * @tparam V combiner type
   * @return
   */
  def reduceScatterParallel[U: ClassTag, V: ClassTag](data: U,
                                         splitOp: (U, Int, Int) => V,
                                         reduceOp: (V, V) => V,
                                         concatOp: IndexedSeq[V] => V,
                                         requiredParallelism: Int): V = {
    if (requiredParallelism > parallelism) {
      throw new IllegalArgumentException(s"Required parallelism ${requiredParallelism} is greater than communicator parallelism ${parallelism}")
    }
    val futures = (0 until requiredParallelism).map(tid => {
      Future {
               val tx = getTx(tid)
               val rx = getRx(tid)
               val tlc = _tlc.get()
               val localData = (0 until size).map(i => splitOp(data, i * requiredParallelism + tid, size * requiredParallelism))
               tlc.sendObject(tx, localData((rank + size - 1) % size))
               for (i <- 2 until size) {
                 val received = tlc.recvObject[V](rx)
                 val local = localData((rank + size - i) % size)
                 tlc.sendObject(tx, reduceOp(received, local))
               }
               val received = tlc.recvObject[V](rx)
               val local = localData(rank)
               reduceOp(received, local)
             }
    })
    concatOp(Await.result(Future.sequence(futures), Duration.Inf))
  }

  /**
   * Perform Allgather communication on the communicator
   *
   * @param input_data           the input data as a byte array to be allgather
   * @param required_parallelism Required parallelism level.
   *                             Increased parallelism level would improve the performance with the price that the
   *                             input data will be sliced into more smaller parts.
   * @return a group of byte arrays as the result of allgather
   */
  def allgatherParallel(input_data: Array[Byte], required_parallelism: Int = parallelism): Array[Array[Byte]] = {
    if (required_parallelism > parallelism) {
      throw new IllegalArgumentException(s"Required parallelism ${required_parallelism} is greater than communicator parallelism ${parallelism}")
    }
    val futures = (0 until required_parallelism).map(i => {
      Future {
               val bs = input_data.size / required_parallelism
               val fromIdx = i * bs
               val toIdx = if (i == (required_parallelism - 1)) input_data.size else (i + 1) * bs
               val data = input_data.slice(fromIdx, toIdx)
               val tx = getTx(i)
               val rx = getRx(i)
               val result = new Array[Array[Byte]](size)
               result(rank) = data
               tx.send(data, 0)
               for (i <- 1 until size - 1) {
                 val received = rx.recv(0)
                 result((rank + size - i) % size) = received
                 tx.send(received, 0)
               }
               val received = rx.recv(0)
               result((rank + 1) % size) = received
               result.foreach(e => require(e != null))
               result
             }(_tp)
    })
    val result_slices = Await.result(Future.sequence(futures), Duration.Inf)
    // val result_slices = futures.map(f => Await.result(f, Duration.Inf))
    val result_lengths = result_slices.map(_.map(_.length).toArray).reduce((l, r) => {
      require(l.length == size)
      require(r.length == size)
      l.zip(r).map(t => t._1 + t._2)
    })
    val results = result_lengths.map(l => new Array[Byte](l))
    (0 until size).map(i =>
                         Future {
                                  var offset = 0
                                  for (j <- 0 until required_parallelism) {
                                    val slice = result_slices(j)(i)
                                    System.arraycopy(slice, 0, results(i), offset, slice.length)
                                    offset += slice.length
                                  }
                                }(_tp)
                       ).foreach(f => Await.result(f, Duration.Inf))
    results
  }
}

