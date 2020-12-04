package org.amazon.reviews.transform

import org.amazon.reviews.SparkFactory
import org.amazon.reviews.config.CmdConfig
import org.amazon.reviews.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import scopt.OParser.parse

object DeduplicateData {

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

    val reviews: Dataset[Review] = spark.read.parquet(s"${config.sourceDir}/reviews").as[Review]

    val metadata: Dataset[Metadata] = spark.read.parquet(s"${config.sourceDir}/metadata").as[Metadata]

    val distinctReviews: Dataset[Review] = reviews.distinct

    val distinctMetadata: Dataset[Metadata] = metadata.distinct

    distinctReviews
      .repartition(1000)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.targetDir}/reviews_dedup")

    distinctMetadata
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.targetDir}/metadata_dedup")
  }

}
