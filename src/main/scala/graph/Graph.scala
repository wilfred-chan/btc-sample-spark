package graph

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object GraphMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nsample.Graph <input dir> <output dir>")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Extract")
      .getOrCreate()
    // This import is needed to use the $-notation
    import spark.implicits._

    val original = spark.read.parquet(args(0));
    // extract
    val extract = original.select($"hash", $"input_count", $"output_count", $"input_value", $"output_value", $"date", $"fee");
    // flatten
    val input_add = original.select($"hash", explode($"inputs.address").alias("input_add"));
    val output_add = original.select($"hash", explode($"outputs.address").alias("output_add"));
    // join together
    val result = extract.join(input_add, Seq("hash"), "inner").join(output_add, Seq("hash"), "inner");
    // write to CSV
    result.write.option("header", true).csv(args(1));

  }
}
