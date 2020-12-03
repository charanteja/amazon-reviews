package org.amazon.reviews.load

import org.apache.log4j.Level
import org.apache.log4j.Logger

import org.amazon.reviews.SparkFactory
import org.amazon.reviews.config.CmdConfig
import org.amazon.reviews.types.Review
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OParser.parse

object LoadReviews {

  Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

  def main(args: Array[String]) {
    parse(CmdConfig.parserSpec(), args, CmdConfig()) match {
      case Some(config) =>
        // Define spark sql warehouse dir and hive metastore at runtime
        implicit val spark: SparkSession = SparkFactory.createSparkSession("Extract Reviews")
        extract(config)
        SparkFactory.stopSparkSession(spark)
      case _ => System.exit(1)
    }
  }

  def extract(config: CmdConfig)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val reviews: Dataset[Review] = spark.readStream.json(s"${config.sourceDir}").as[Review]
    val query: StreamingQuery = reviews
      .writeStream
      .format("parquet")
      .option("path", s"${config.targetDir}")
      .start()
    query.awaitTermination()
  }
}
