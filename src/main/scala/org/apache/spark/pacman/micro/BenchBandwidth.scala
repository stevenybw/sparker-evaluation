package org.apache.spark.pacman.micro

import org.apache.spark.SparkEnv
import org.apache.spark.pacman.example.AbstractParams
import org.apache.spark.sparkle.SparkleContext
import org.apache.spark.sql.SparkSession
import org.zeromq.SocketType
import scopt.OptionParser

object BenchBandwidth {
  case class Params(
                     maxParallelism: Int = 8,
                     fromSize: Int = 8,
                     toSize: Int = 64*1024*1024,
                     port: Int = 2119,
                     queueDepth: Int = 32
                   ) extends AbstractParams[Params]

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("BenchBandwidth") {
      head("BenchBandwidth: measure the ReduceScatter performance of Sparkle")
      opt[Int]("maxParallelism")
        .action((x, c) => c.copy(maxParallelism = x))
      opt[Int]("fromSize")
        .action((x, c) => c.copy(fromSize = x))
      opt[Int]("toSize")
        .action((x, c) => c.copy(toSize = x))
      opt[Int]("queueDepth")
        .action((x, c) => c.copy(queueDepth = x))
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
   * @param baseSrcPort the basic TCP/IP port number
   *                 Must ensure that [bp, bp+parallelism) is available
   * @param messageBytes bytes to communicate
   * @param queueDepth the number of concurrent outstanding sends
   * @param parallelism the number of threads participated in the communication
   * @return
   */
  def measureBandwidth(spc: SparkleContext, srcRank: Int, srcHost: String, dstRank: Int, dstHost: String, basePort: Int, messageBytes: Int, parallelism: Int, totalBytes: Int = 1024*1024*1024, maxAttempts: Int = 1024*1024): (Double, Double) = {
    var queueDepth = totalBytes / messageBytes
    queueDepth = Math.min(queueDepth, maxAttempts)
    queueDepth = Math.max(queueDepth, 1)
    val mbps: Array[(Double, Double)] = spc.spawnZmq(comm => {
      val rank = comm.rank
      val topo = comm.topology
      val host = SparkEnv.get.blockManager.blockManagerId.host
      val ctx = comm.ctx
      val baseSrcPort = basePort+2
      val baseDestPort = if (srcHost == dstHost) baseSrcPort+parallelism else baseSrcPort
      val srcControlPort = basePort
      val destControlPort = basePort+1
      if (rank == srcRank) {
        require(host == srcHost)

        val control_tx = ctx.socket(SocketType.PUSH)
        control_tx.bind(s"tcp://*:${srcControlPort}")
        val control_rx = ctx.socket(SocketType.PULL)
        control_rx.connect(s"tcp://${dstHost}:${destControlPort}")

        val r = new scala.util.Random(2019)
        val data = new Array[Byte](messageBytes)
        r.nextBytes(data)

        /** Establish P parallel connections */
        val tx_list = (0 until parallelism).map(tid => {
          val tx = ctx.socket(SocketType.PUSH)
          tx.bind(s"tcp://*:${baseSrcPort + tid}")
          tx
        })
        val rx_list = (0 until parallelism).map(tid => {
          val rx = ctx.socket(SocketType.PULL)
          rx.connect(s"tcp://${dstHost}:${baseDestPort + tid}")
          rx
        })

        control_tx.send("syn")
        val s = control_rx.recvStr()
        require(s == "ack")

        val beginTime = System.nanoTime
        val threads = (0 until parallelism).map(tid => new Thread(
          new Runnable {
            override def run(): Unit = {
              val tx = tx_list(tid)
              val rx = rx_list(tid)

              for (z <- 0 until queueDepth) {
                tx.send(data)
              }
              val reply = rx.recvStr()
              require(reply == "fin", s"Received message '${reply}' != 'fin'")
            }
          }))
        threads.foreach(_.start())
        threads.foreach(_.join())
        val endTime = System.nanoTime
        tx_list.foreach(_.close())
        rx_list.foreach(_.close())
        control_tx.close()
        control_rx.close()
        (1e-9*(endTime-beginTime), 1.0 * parallelism * messageBytes.toDouble * queueDepth.toDouble / (1e-3 * (endTime - beginTime)))
      } else if (rank == dstRank) {
        require(host == dstHost)
        val control_tx = ctx.socket(SocketType.PUSH)
        control_tx.bind(s"tcp://*:${destControlPort}")
        /** Establish P parallel connections */
        val tx_list = (0 until parallelism).map(tid => {
          val tx = ctx.socket(SocketType.PUSH)
          tx.bind(s"tcp://*:${baseDestPort + tid}")
          tx
        })

        val control_rx = ctx.socket(SocketType.PULL)
        control_rx.connect(s"tcp://${srcHost}:${srcControlPort}")
        val rx_list = (0 until parallelism).map(tid => {
          val rx = ctx.socket(SocketType.PULL)
          rx.connect(s"tcp://${srcHost}:${baseSrcPort + tid}")
          rx
        })

        val s = control_rx.recvStr()
        require(s == "syn")
        control_tx.send("ack")
        val beginTime = System.nanoTime
        val threads = (0 until parallelism).map(tid => new Thread(
          new Runnable {
            override def run(): Unit = {
              val tx = tx_list(tid)
              val rx = rx_list(tid)

              for (z <- 0 until queueDepth) {
                val s = rx.recv()
                require(s.length == messageBytes)
              }
              tx.send("fin")
            }
          }))
        threads.foreach(_.start())
        threads.foreach(_.join())
        val endTime = System.nanoTime
        tx_list.foreach(_.close())
        rx_list.foreach(_.close())
        control_tx.close()
        control_rx.close()
        (1e-9*(endTime-beginTime), 1.0 * parallelism * messageBytes.toDouble * queueDepth.toDouble / (1e-3 * (endTime - beginTime)))
      } else {
        (0.0, 0.0)
      }
    })
    mbps(srcRank)
  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName(s"BenchBandwidth with $params")
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

//    {
//      var bytes = 16
//      while (bytes <= 1024*1024) {
//        val bw = measureBandwidth(spc, srcRank, srcHost, intraRank, srcHost, params.port, bytes, params.queueDepth, params.maxParallelism)
//        bytes *= 2
//      }
//    }
//
//    {
//      var parallelism = 1
//      while (parallelism <= params.maxParallelism) {
//        var bytes = params.fromSize
//        while (bytes <= params.toSize) {
//          val (duration, mbps) = measureBandwidth(spc, srcRank, srcHost, intraRank, srcHost, params.port, bytes, params.queueDepth, parallelism)
//          println(f"intra ${parallelism}%2d ${bytes}%9d ${duration}%3.6f ${mbps}%6.3f")
//          bytes *= 2
//        }
//        parallelism *= 2
//      }
//    }

    {
      var bytes = 16
      while (bytes <= 1024*1024) {
        val bw = measureBandwidth(spc, srcRank, srcHost, interRank, dstHost, params.port, bytes, params.maxParallelism)
        bytes *= 2
      }
    }

    {
      var parallelism = 1
      while (parallelism <= params.maxParallelism) {
        var bytes = params.fromSize
        while (bytes <= params.toSize) {
          val (duration, mbps) = measureBandwidth(spc, srcRank, srcHost, interRank, dstHost, params.port, bytes, parallelism)
          println(f"inter ${parallelism}%2d ${bytes}%9d ${duration}%3.6f ${mbps}%6.3f")
          bytes *= 2
        }
        parallelism *= 2
      }
    }
  }
}
