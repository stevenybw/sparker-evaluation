package org.apache.spark.pacman.micro

// scalastyle:off println

import org.apache.spark.pacman.example.AbstractParams
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

object BenchWordCount {

  case class Params(topk: Int = 128,
                    input: String = null) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("BenchWordCount") {
      head("BenchWordCount")
      opt[Int]("topk")
        .text(s"the number of top words to show: ${defaultParams.topk}")
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

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"BenchWordCount with $params")
      .getOrCreate()
    val sc = spark.sparkContext

    println(s"BenchWordCount with parameters:\n$params")

    val parallelism = spark.conf.get("spark.default.parallelism", "")
    println(s"Parallelism is ${parallelism}")

    val reg = "[\u0080-\uFFFF\\s]".r
    val bt = System.currentTimeMillis
    val words = sc.textFile(params.input).flatMap(x => reg.split(x).filter(x => x.length > 0))
    val wordCount = words.map(x => (x, 1L)).reduceByKey(_+_)
    val topK = wordCount.map(x => (x._2, x._1)).top(params.topk)
    val et = System.currentTimeMillis

    topK.sorted.map(x => s"${x._2} - ${x._1}").foreach(x => println(x))

    println(s"Word-count time: ${1e-3 * (et - bt)} second")
    spark.stop()
  }
}

// scalastyle:on println

