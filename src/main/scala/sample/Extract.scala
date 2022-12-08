package sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object Extract {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nsample.Extract <input dir> <output dir>")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Extract")
      .getOrCreate()
    // This import is needed to use the $-notation
    import spark.implicits._

    val original = spark.read.parquet(args(0));
    // filter out non-1-to-1 transaction (46484 rows)
    val filtered = original.filter($"input_count" === 1 && $"output_count" === 1);
    // extract
    val extract = filtered.select($"hash", $"input_count", $"output_count", $"is_coinbase", $"input_value", $"output_value", $"date", $"fee");
    // flatten
    val input_add = filtered.select($"hash", explode($"inputs.address").alias("input_address"));
    val output_add = filtered.select($"hash", explode($"outputs.address").alias("output_address"));
    // join together
    val result = extract.join(input_add, Seq("hash"), "inner").join(output_add, Seq("hash"),"inner");
    // write to CSV
    result.write.option("header", false).csv(args(1));

//    val df1 = result.as("df1").select($"input_address");
//    val df2 = result.as("df2").select($"output_address");
//    val df3 = result.as("df3").select($"input_address");
//    val final_result = df1.join(df2, $"df1.input_address" === $"df2.output_address");
//    // write to CSV
//    final_result.write.option("header", true).csv(args(1));

  }
}