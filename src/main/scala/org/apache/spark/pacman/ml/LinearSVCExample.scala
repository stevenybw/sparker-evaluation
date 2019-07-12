package org.apache.spark.pacman.ml

// scalastyle:off println

import scopt.OptionParser

import scala.collection.mutable
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{LinearSVC, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}

object LinearSVCExample {

  case class Params(
    input: String = null,
    testInput: String = "",
    dataFormat: String = "libsvm",
    regParam: Double = 0.1,
    maxIter: Int = 100,
    fitIntercept: Boolean = true,
    tol: Double = 1E-6,
    fracTest: Double = 0.2,
    computeTest: Boolean = false) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LinearSVCExample") {
      head("LinearSVCExample")
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))
      opt[Int]("maxIter")
        .text(s"maximum number of iterations, default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Boolean]("fitIntercept")
        .text(s"whether to fit an intercept term, default: ${defaultParams.fitIntercept}")
        .action((x, c) => c.copy(fitIntercept = x))
      opt[Double]("tol")
        .text(s"the convergence tolerance of iterations, Smaller value will lead " +
          s"to higher accuracy with the cost of more iterations, default: ${defaultParams.tol}")
        .action((x, c) => c.copy(tol = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing. If given option testInput, " +
          s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[String]("testInput")
        .text(s"input path to test dataset. If given, option fracTest is ignored." +
          s" default: ${defaultParams.testInput}")
        .action((x, c) => c.copy(testInput = x))
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
      checkConfig { params =>
        if (params.fracTest < 0 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"LinearSVCExample with $params")
      .getOrCreate()

    println(s"LinearSVCExample with parameters:\n$params")

    // Load training and test data and cache it.
    val (training: DataFrame, test: DataFrame) = DecisionTreeExample.loadDatasets(params.input,
      params.dataFormat, params.testInput, "classification", params.fracTest)

    // Set up Pipeline.
    val stages = new mutable.ArrayBuffer[PipelineStage]()

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
    stages += labelIndexer

    val svm = new LinearSVC()
      .setFeaturesCol("features")
      .setLabelCol("indexedLabel")
      .setRegParam(params.regParam)
      .setMaxIter(params.maxIter)
      .setTol(params.tol)
      .setFitIntercept(params.fitIntercept)

    stages += svm
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline.
    val startTime = System.nanoTime()
    val pipelineModel = pipeline.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    if (params.computeTest) {
      println("Test data results:")
      DecisionTreeExample.evaluateClassificationModel(pipelineModel, test, "indexedLabel")
    }

    spark.stop()
  }
}

// scalastyle:on println

