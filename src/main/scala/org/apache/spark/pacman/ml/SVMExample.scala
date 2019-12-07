package org.apache.spark.pacman.ml

// scalastyle:off println

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.pacman.example.AbstractParams
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

import scala.collection.mutable

object SVMExample {

  case class Params(
    input: String = null,
    testInput: String = "",
    dataFormat: String = "libsvm",
    stepSize: Double = 1.0,
    regParam: Double = 0.01,
    miniBatchFrac: Double = 1.0,
    maxIter: Int = 100,
    fitIntercept: Boolean = true,
    tol: Double = 1E-6,
    fracTest: Double = 0.2,
    computeTest: Boolean = false) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("SVMExample") {
      head("SVMExample")
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
      opt[Double]("stepSize")
        .text(s"step size, " +
                s"default: ${defaultParams.stepSize}")
        .action((x, c) => c.copy(stepSize = x))
      opt[Double]("miniBatchFrac")
        .text(s"fraction of minibatch, " +
                s"default: ${defaultParams.miniBatchFrac}")
        .action((x, c) => c.copy(miniBatchFrac = x))
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
      .appName(s"SVMExample with $params")
      .getOrCreate()
    val sc = spark.sparkContext

    println(s"SVMExample with parameters:\n$params")

    // Stub
    spark.sparkContext.parallelize(0 until 2048, 2048).count()
    spark.sparkContext.updateExecutorLocations()
    println(s"Number of executors observed: ${spark.sparkContext.executorLocations.size}")

    val parallelism = spark.conf.get("spark.default.parallelism", "")
    println(s"Parallelism is ${parallelism}")
    val training = MLUtils.loadLibSVMFile(sc, params.input).repartition(parallelism.toInt).persist(StorageLevel.MEMORY_AND_DISK.setStaticScheduling())
//    val test = MLUtils.loadLibSVMFile(sc, params.testInput).repartition(parallelism.toInt).persist(StorageLevel.MEMORY_AND_DISK.setStaticScheduling())
    val t0 = System.nanoTime()
    val svm = SVMWithSGD.train(training, params.maxIter, params.stepSize, params.regParam, params.miniBatchFrac)
    val t1 = System.nanoTime()

    println(s"Training time: ${1.0e-9*(t1-t0)} seconds")

    spark.stop()
  }
}

// scalastyle:on println

