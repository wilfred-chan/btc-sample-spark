package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object WordCountMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val data = textFile.flatMap(value => value.split(" "))
                       .filter(_.length > 0)
                       .filter(word => word.charAt(0).toLower >= 'm' && word.charAt(0).toLower <= 'q')
                       .map(word => (word, 1))
                       .reduceByKey(_ + _)
                       .sortByKey()
                       .sortBy(_._2, false)

    val top100 = sc.parallelize(data.take(100))

    logger.info(data.toDebugString)


    top100.saveAsTextFile(args(1))
  }
}