package org.apache.spark.pacman.micro

import org.apache.spark.pacman.example.{AbstractParams, Benchmark}
import org.apache.spark.sparkle.SparkleContext
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object BenchAggregation {
  case class Params(
                   maxParallelism: Int = 8,
                   numVectorPerPartition: Int = 1,
                   numPartition: Int = 192,
                   numAttempts: Int = 32,
                   fromDimension: Int = 128,
                   toDimension: Int = 64*1024*1024,
                   executorSortedByHost: Boolean = true) extends AbstractParams[Params]

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("BenchAggregation") {
      head("BenchAggregation: measure the ReduceScatter performance of Sparkle")
      opt[Int]("maxParallelism")
        .action((x, c) => c.copy(maxParallelism = x))
      opt[Int]("numVectorPerPartition")
        .action((x, c) => c.copy(numVectorPerPartition = x))
      opt[Int]("numPartition")
        .action((x, c) => c.copy(numPartition = x))
      opt[Int]("numAttempts")
        .action((x, c) => c.copy(numAttempts = x))
      opt[Int]("fromDimension")
        .action((x, c) => c.copy(fromDimension = x))
      opt[Int]("toDimension")
        .action((x, c) => c.copy(toDimension = x))
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
      .appName(s"BenchAggregation with $params")
      .getOrCreate()

    SparkleContext.executorSortedByHost = params.executorSortedByHost
    SparkleContext.numSockets = params.maxParallelism
    val spc = SparkleContext.getOrCreate(spark.sparkContext)
    Benchmark.rddAggregate(spc, params.numVectorPerPartition, params.numPartition, params.numAttempts, params.maxParallelism, params.fromDimension, params.toDimension)
  }
}
