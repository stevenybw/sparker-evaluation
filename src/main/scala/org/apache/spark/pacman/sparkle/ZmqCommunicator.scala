package org.apache.spark.pacman.sparkle

import java.nio.ByteBuffer
import java.util.concurrent.Executors

import org.apache.spark.serializer.{Serializer, SerializerInstance}
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
 * @param topology A indexed sequence from rank to (Host, Ports, ExecutorId)
 */
class ZmqCommunicator(val ctx: ZMQ.Context, val rank: Int, val topology: IndexedSeq[(String, IndexedSeq[Int], String)], txAndRx: IndexedSeq[(ZMQ.Socket, ZMQ.Socket)], _serializer: Serializer) {
  private val _tlsi = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = _serializer.newInstance()
  }

  private val _parallelism = txAndRx.size

  topology.foreach(e => require(e._2.size == _parallelism))

  /** The degree of parallelism of the communicator */
  def parallelism: Int = _parallelism

  private implicit val _tp: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism))

  /** The size of the communicator */
  def size: Int = topology.size

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
    for (i <- 1 until size-1) {
      val received = rx.recv(0)
      result((rank+size-i) % size) = received
      tx.send(received, 0)
    }
    val received = rx.recv(0)
    result((rank+1) % size)=received
    result.foreach(e => require(e != null))
    result
  }

  /**
   * ReduceScatter with only one thread
   * @param data the input data for each destination
   * @param reduceOp how the two element can be reduced
   * @tparam T
   * @return a single element that has been reduced
   */
  def reduceScatter[T: ClassTag](data: IndexedSeq[T], reduceOp: (T, T)=>T): T = {
    val _si = _tlsi.get()
    require(data.size == size)
    val tx = getTx(0)
    val rx = getRx(0)
    tx.send(_si.serialize(data((rank+size-1) % size)).array(), 0)
    for (i <- 2 until size) {
      val received = _si.deserialize[T](ByteBuffer.wrap(rx.recv(0)))
      val toSend = reduceOp(data((rank+size-i) % size), received)
      tx.send(_si.serialize(toSend).array(), 0)
    }
    val received = _si.deserialize[T](ByteBuffer.wrap(rx.recv(0)))
    reduceOp(data(rank), received)
  }

  /**
   * ReduceScatter in parallel
   * @param data the input data
   *             Every participant's data must be in the same dimension
   * @param reduceOp how two element can be reduced
   * @tparam T
   * @return
   */
  def reduceScatterParallel[T: ClassTag](data: Array[T], reduceOp: (T, T)=>T, required_parallelism: Int = parallelism): Array[T] = {
    if (required_parallelism > parallelism) {
      throw new IllegalArgumentException(s"Required parallelism ${required_parallelism} is greater than communicator parallelism ${parallelism}")
    }
    val bs = data.length / size
    val assignIdx = (0 until size).map(i => (i*bs, if (i == size-1) data.length else (i+1)*bs))
    val futures = (0 until required_parallelism).map(tid => {
      Future {
        val tx = getTx(tid)
        val rx = getRx(tid)
        val si = _tlsi.get()
        val localData = (0 until size).map(i => {
          val (bi, ei) = assignIdx(i)
          val sz = ei - bi
          val bs = sz / required_parallelism
          data.slice(bi+tid*bs, if (tid==required_parallelism-1) ei else bi+(tid+1)*bs)
        })
        tx.send(si.serialize(localData((rank+size-1)%size)).array(), 0)
        for (i <- 2 until size) {
          val received = si.deserialize[Array[T]](ByteBuffer.wrap(rx.recv(0)))
          val local = localData((rank+size-i)%size)
          require(received.length == local.length)
          for (j <- local.indices) {
            received(j) = reduceOp(received(j), local(j))
          }
          tx.send(si.serialize(received).array(), 0)
        }
        val received = si.deserialize[Array[T]](ByteBuffer.wrap(rx.recv(0)))
        val local = localData(rank)
        for (j <- local.indices) {
          received(j) = reduceOp(received(j), local(j))
        }
        received
      }(_tp)
    })
    val result_slices = Await.result(Future.sequence(futures), Duration.Inf)
    val result_length = result_slices.map(_.length).sum
    val result = new Array[T](result_length)
    var offset = 0
    for (tid <- 0 until required_parallelism) {
      System.arraycopy(result_slices(tid), 0, result, offset, result_slices(tid).length)
      offset += result_slices(tid).length
    }
    result
  }

  /**
   * Perform Allgather communication on the communicator
   * @param input_data the input data as a byte array to be allgather
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
        val fromIdx = i*bs
        val toIdx = if (i==(required_parallelism-1)) input_data.size else (i+1)*bs
        val data = input_data.slice(fromIdx, toIdx)
        val tx = getTx(i)
        val rx = getRx(i)
        val result = new Array[Array[Byte]](size)
        result(rank) = data
        tx.send(data, 0)
        for (i <- 1 until size-1) {
          val received = rx.recv(0)
          result((rank+size-i) % size) = received
          tx.send(received, 0)
        }
        val received = rx.recv(0)
        result((rank+1) % size)=received
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

