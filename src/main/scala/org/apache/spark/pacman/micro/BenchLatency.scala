package org.apache.spark.pacman.micro

import org.apache.spark.SparkEnv
import org.apache.spark.pacman.example.AbstractParams
import org.apache.spark.sparkle.SparkleContext
import org.apache.spark.sql.SparkSession
import org.zeromq.SocketType
import scopt.OptionParser

object BenchLatency {
  case class Params(
                     fromSize: Int = 8,
                     toSize: Int = 1024*1024,
                     port: Int = 2019,
                     numAttempts: Int = 32
                   ) extends AbstractParams[Params]

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("BenchLatency") {
      head("BenchLatency: measure the ReduceScatter performance of Sparkle")
      opt[Int]("fromSize")
        .action((x, c) => c.copy(fromSize = x))
      opt[Int]("toSize")
        .action((x, c) => c.copy(toSize = x))
      opt[Int]("numAttempts")
        .action((x, c) => c.copy(numAttempts = x))
      opt[Int]("port")
        .action((x, c) => c.copy(port = x))

      checkConfig { params => success }
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  /**
   * Measure the latency between a pair of actors
   *
   * @param srcRank the rank of source actor
   * @param srcHost the host of source actor
   * @param dstRank the rank of destination actor
   * @param dstHost the host of destination actor
   * @param port the TCP/IP port number
   * @param messageBytes bytes to communicate
   * @param numTries number of attempts
   * @return
   */
  def measureLatencyUs(spc: SparkleContext, srcRank: Int, srcHost: String, dstRank: Int, dstHost: String, port: Int, messageBytes: Int, numTries: Int): Double = {
    val latencies: Array[Double] = spc.spawnZmq(comm => {
      val rank = comm.rank
      val topo = comm.topology
      val host = SparkEnv.get.blockManager.blockManagerId.host
      val ctx = comm.ctx
      val destPort = if (srcHost == dstHost) port+1 else port
      if (rank == srcRank) {
        require(host == srcHost)
        val tx = ctx.socket(SocketType.PUSH)
        tx.bind(s"tcp://*:${port}")
        val rx = ctx.socket(SocketType.PULL)
        rx.connect(s"tcp://${dstHost}:${destPort}")

        val r = new scala.util.Random(2019)
        val data = new Array[Byte](messageBytes)
        r.nextBytes(data)

        tx.send("syn")
        val s = rx.recvStr()
        require(s == "ack")

        val beginTime = System.nanoTime
        for (z <- 0 until numTries) {
          tx.send(data)
          val s = rx.recv()
          require(s.size == data.size)
        }
        val endTime = System.nanoTime

        tx.close()
        rx.close()
        1e-3 / 2 * (endTime - beginTime) / numTries
      } else if (rank == dstRank) {
        require(host == dstHost)
        val tx = ctx.socket(SocketType.PUSH)
        tx.bind(s"tcp://*:${destPort}")
        val rx = ctx.socket(SocketType.PULL)
        rx.connect(s"tcp://${srcHost}:${port}")

        val s = rx.recvStr()
        require(s == "syn")
        tx.send("ack")

        val beginTime = System.nanoTime
        for (z <- 0 until numTries) {
          val data = rx.recv()
          require(data.size == messageBytes)
          tx.send(data)
        }
        val endTime = System.nanoTime
        tx.close()
        rx.close()
        1e-3 / 2 * (endTime - beginTime) / numTries
      } else {
        0.0
      }
    })
    latencies(srcRank)
  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName(s"BenchLatency with $params")
      .getOrCreate()

    val spc = SparkleContext.getOrCreate(spark.sparkContext)
    val rankToHost = spc.spawnZmq(comm => {
      val rank = comm.rank
      val host = comm.topology(rank)._1
      host
    })
    val srcRank = 0
    val intraRank = 1
    val srcHost = rankToHost(srcRank)
    val (dstHost, interRank) = rankToHost.zipWithIndex.filter(_._1 != srcHost).head
    require(rankToHost(intraRank) == srcHost)
    require(srcHost != dstHost)
    println(s"source:  rank = ${srcRank}  host = ${srcHost}")
    println(s"intra:   rank = ${intraRank}  host = ${srcHost}")
    println(s"inter:   rank = ${interRank}  host = ${dstHost}")

    {
      var bytes = params.fromSize
      while (bytes <= params.toSize) {
        val lat = measureLatencyUs(spc, srcRank, srcHost, intraRank, srcHost, params.port, bytes, params.numAttempts)
        bytes *= 2
      }
    }

    {
      var bytes = params.fromSize
      while (bytes <= params.toSize) {
        val lat = measureLatencyUs(spc, srcRank, srcHost, intraRank, srcHost, params.port, bytes, params.numAttempts)
        println(f"intra ${bytes}%9d ${lat}%9.3f")
        bytes *= 2
      }
    }

    {
      var bytes = params.fromSize
      while (bytes <= params.toSize) {
        val lat = measureLatencyUs(spc, srcRank, srcHost, interRank, dstHost, params.port, bytes, params.numAttempts)
        bytes *= 2
      }
    }

    {
      var bytes = params.fromSize
      while (bytes <= params.toSize) {
        val lat = measureLatencyUs(spc, srcRank, srcHost, interRank, dstHost, params.port, bytes, params.numAttempts)
        println(f"inter ${bytes}%9d ${lat}%9.3f")
        bytes *= 2
      }
    }
  }
}
