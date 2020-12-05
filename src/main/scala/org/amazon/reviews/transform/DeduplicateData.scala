package org.amazon.reviews.transform

import org.amazon.reviews.RunSpark
import org.amazon.reviews.config.CmdConfig
import org.amazon.reviews.types._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * Spark batch job to deduplicate data
  * Since deduplication needs to be on full data,
  * its better to have a batch job that runs at
  * fixed interval
  */
object DeduplicateData extends RunSpark {

  /**
    * Using type as parameter to deduplicate function
    */
  def deduplicate[T](input: Dataset[T])(implicit spark: SparkSession): Dataset[T] = {

    input.distinct
  }

  def extract(config: CmdConfig)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    logger.info("Reading reviews data")
    val reviews: Dataset[Review] = spark.read.parquet(s"${config.sourceDir}/reviews").as[Review]

    logger.info("Reading metadata data")
    val metadata: Dataset[Metadata] = spark.read.parquet(s"${config.sourceDir}/metadata").as[Metadata]

    logger.info("Deduplicating reviews data")
    val distinctReviews: Dataset[Review] = deduplicate[Review](reviews)

    logger.info("Deduplicating metadata data")
    val distinctMetadata: Dataset[Metadata] = deduplicate[Metadata](metadata)

    /**
      * Repartition data to make sure data is distributed
      * across multiple files with optimum file sizes
      */
    logger.info("Writing deduplicated reviews data")
    distinctReviews
      .repartition(1000)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.targetDir}/reviews_dedup")

    logger.info("Writing deduplicated metadata data")
    distinctMetadata
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.targetDir}/metadata_dedup")
  }

}
