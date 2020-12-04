package org.amazon.reviews.load

import org.amazon.reviews.SparkFactory
import org.amazon.reviews.config.CmdConfig
import org.amazon.reviews.types.Metadata
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OParser.parse

object LoadMetadata {

  def main(args: Array[String]) {
    parse(CmdConfig.parserSpec(), args, CmdConfig()) match {
      case Some(config) =>
        implicit val spark: SparkSession = SparkFactory.sparkSession
        extract(config)
        SparkFactory.stopSparkSession()
      case _ => System.exit(1)
    }
  }

  def extract(config: CmdConfig)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val metadata: Dataset[Metadata] = spark
      .readStream
      .json(s"${config.sourceDir}")
      .drop("salesRank", "_corrupt_record")
      .as[Metadata]
    /***
      * 1. Number of partitions to use can be optimized by size of data
      * 2. Stream is processed in micro-batch mode, micro-batch interval can be optimized based
      * on how much we want each batch parquet file size to be
      */
    val query: StreamingQuery = metadata
      .writeStream
      .format("parquet")
      .option("path", s"${config.targetDir}")
      .start()
    query.awaitTermination()
  }
}
