package org.apache.spark.pacman.ml

// scalastyle:off println

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.pacman.example.AbstractParams
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

import scala.collection.mutable

object LDAExample {

  case class Params(
    input: String = null,
    dataFormat: String = "libsvm",
    K: Int = 10,
    maxIter: Int = 100,
    computeTest: Boolean = false) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LDAExample") {
      head("LDAExample: an example LDA.")
      opt[Int]("K")
        .text(s"number of topics, default: ${defaultParams.K}")
        .action((x, c) => c.copy(K = x))
      opt[Int]("maxIter")
        .text(s"maximum number of iterations, default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[String]("dataFormat")
        .text("data format: libsvm (default), dense (deprecated in Spark v1.1)")
        .action((x, c) => c.copy(dataFormat = x))
      opt[Boolean]("computeTest")
        .text("whether to compute the perplexity of the dataset (which may be very costly)")
        .action((x, c) => c.copy(computeTest = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
      checkConfig { params => success }
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def initStaticScheduling(spark: SparkSession): Unit = {
    // Stub for forcefully static-scheduling
    spark.sparkContext.parallelize(0 until 1024, 1024).count()
    spark.sparkContext.updateExecutorLocations()
    println(s"Number of executors observed: ${spark.sparkContext.executorLocations.size}")
  }

  def processDataset(spark: SparkSession, df: DataFrame): Dataset[Row] = {
    val parallelism = spark.conf.get("spark.default.parallelism", "")
    println(s"Parallelism is ${parallelism}")
    if (!parallelism.isEmpty) {
      df.repartition(parallelism.toInt).persist(StorageLevel.MEMORY_AND_DISK.setStaticScheduling())
    } else {
      df.persist(StorageLevel.MEMORY_AND_DISK.setStaticScheduling())
    }
  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"LDAExample with $params")
      .getOrCreate()

    println(s"LDAExample with parameters:\n$params")

    initStaticScheduling(spark)
    // Load training and test data and cache it.
    val dataset_i = spark.read.format(params.dataFormat)
      .load(params.input)
    val dataset = processDataset(spark, dataset_i)


    // Trans a LDA model
    val lda = new LDA().setK(params.K).setMaxIter(params.maxIter)
    val model = lda.fit(dataset)

    if (params.computeTest) {
      val ll = model.logLikelihood(dataset)
      val lp = model.logPerplexity(dataset)
      println(s"The lower bound on the log likelihood of the entire corpus: $ll")
      println(s"The upper bound on perplexity: $lp")
    }

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)
    // $example off$

    spark.stop()
  }
}

// scalastyle:on println
