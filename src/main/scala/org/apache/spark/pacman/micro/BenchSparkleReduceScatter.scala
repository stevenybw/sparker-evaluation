package org.apache.spark.pacman.micro

import org.apache.spark.pacman.example.{AbstractParams, Benchmark}
import org.apache.spark.sparkle.SparkleContext
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object BenchSparkleReduceScatter {
  case class Params(
                   maxParallelism: Int = 8,
                   fromSize: Int = 128,
                   toSize: Int = 64*1024*1024,
                   numAttempts: Int = 32,
                   verySparseMessage: Boolean = false,
                   executorSortedByHost: Boolean = true
                   ) extends AbstractParams[Params]

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("BenchSparkleReduceScatter") {
      head("BenchSparkleReduceScatter: measure the ReduceScatter performance of Sparkle")
      opt[Int]("maxParallelism")
        .action((x, c) => c.copy(maxParallelism = x))
      opt[Int]("fromSize")
        .action((x, c) => c.copy(fromSize = x))
      opt[Int]("toSize")
        .action((x, c) => c.copy(toSize = x))
      opt[Int]("numAttempts")
        .action((x, c) => c.copy(numAttempts = x))
      opt[Boolean]("verySparseMessage")
        .action((x, c) => c.copy(verySparseMessage = x))
      opt[Boolean]("executorSortedByHost")
        .action((x, c) => c.copy(executorSortedByHost = x))
      checkConfig { params => success }
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName(s"BenchSparkleReduceScatter with $params")
      .getOrCreate()

    SparkleContext.executorSortedByHost = params.executorSortedByHost
    SparkleContext.numSockets = params.maxParallelism
    val spc = SparkleContext.getOrCreate(spark.sparkContext)
    Benchmark.zmqReduceScatterThroughput(spc, params.maxParallelism, params.fromSize, params.toSize, params.numAttempts, params.verySparseMessage)
  }
}
