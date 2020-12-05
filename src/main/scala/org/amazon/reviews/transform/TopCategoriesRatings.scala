package org.amazon.reviews.transform

import org.amazon.reviews.RunSpark
import org.amazon.reviews.config.CmdConfig
import org.amazon.reviews.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

//TODO Spark streaming job to do the same computation
/**
  * Spark batch job to compute top category ratings
  * Batch job can be executed on deduplicated data
  * to produce accurate results.
  * To reduce latency, same job can be implemented using
  * spark streaming by same computation on each micro batch
  * and merging the results of all micro-batches.
  */
object TopCategoriesRatings extends RunSpark {

  def extractProductReviews(reviews: Dataset[Review])(implicit spark: SparkSession): Dataset[ProductReview] = {
    import spark.implicits._

    logger.info("Extracting product reviews from reviews")
    reviews
      .select($"asin", $"reviewTimestamp", $"overall".as("rating"))
      .as[ProductReview]
  }

  def extractProductCategories(metadata: Dataset[Metadata])(implicit spark: SparkSession): Dataset[ProductCategory] = {
    import spark.implicits._

    logger.info("Extracting product categories from metadata")
    val productsWithCategories: DataFrame = metadata
      .select($"asin", explode($"categories").as("categories"))
    productsWithCategories
      .select($"asin", explode($"categories").as("category"))
      .distinct
      .as[ProductCategory]
  }

  def computeTopCategoryRatings(productReviews: Dataset[ProductReview],
                                productCategories: Dataset[ProductCategory],
                                topN: Int = 5)(implicit spark: SparkSession): Dataset[CategoryRating] = {
    import spark.implicits._

    logger.info("Joining product reviews with product categories")
    val productReviewsWithCategory: DataFrame = productReviews.join(productCategories, Seq("asin"))

    logger.info("Computing month by month metrics: total_sold_products, average_rating")
    val categoriesMonth: DataFrame = productReviewsWithCategory.groupBy(
      year($"reviewTimestamp").as("year"),
      month($"reviewTimestamp").as("month"),
      $"category"
    ).agg(
      countDistinct("asin").as("total_sold_products"),
      avg($"rating").as("average_rating")
    )

    logger.info(s"Partitioning by year,month to get top $topN products")
    val w = Window.partitionBy($"year", $"month").orderBy(desc("total_sold_products"))
    categoriesMonth
      .withColumn("rank", rank.over(w))
      .where($"rank" <= topN)
      .drop("rank")
      .as[CategoryRating]
  }

  def extract(config: CmdConfig)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    logger.info("Reading reviews data")
    val reviews: Dataset[Review] = spark.read.parquet(s"${config.sourceDir}/reviews_dedup").as[Review]

    logger.info("Reading metadata data")
    val metadata: Dataset[Metadata] = spark.read.parquet(s"${config.sourceDir}/metadata_dedup").as[Metadata]

    val productReviews: Dataset[ProductReview] = extractProductReviews(reviews)

    val productCategories: Dataset[ProductCategory] = extractProductCategories(metadata)

    val topCategoriesRatings: Dataset[CategoryRating] = computeTopCategoryRatings(productReviews, productCategories)

    logger.info("Writing top categories rating data")
    topCategoriesRatings
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.targetDir}")
  }

}
