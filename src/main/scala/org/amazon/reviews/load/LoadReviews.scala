package org.amazon.reviews.load

import org.amazon.reviews.SparkFactory
import org.amazon.reviews.config.CmdConfig
import org.amazon.reviews.types.Review
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OParser.parse

object LoadReviews {

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

    val reviews: Dataset[Review] = spark
      .readStream
      .json(s"${config.sourceDir}")
      .withColumn("reviewTimestamp", $"unixReviewTime".cast(TimestampType))
      .as[Review]
    val query: StreamingQuery = reviews
      .writeStream
      .format("parquet")
      .option("path", s"${config.targetDir}")
      .start()
    query.awaitTermination()
  }
}
