package org.amazon.reviews.insights

import org.amazon.reviews.SparkFactory
import org.amazon.reviews.config.CmdConfig
import org.amazon.reviews.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import scopt.OParser.parse

object TopCategoriesRatings {

  def main(args: Array[String]) {
    parse(CmdConfig.parserSpec(), args, CmdConfig()) match {
      case Some(config) =>
        implicit val spark: SparkSession = SparkFactory.sparkSession
        extract(config)
        SparkFactory.stopSparkSession()
      case _ => System.exit(1)
    }
  }

  def extractProductReviews(reviews: Dataset[Review])(implicit spark: SparkSession): Dataset[ProductReview] = {
    import spark.implicits._

    reviews
      .select($"asin", $"reviewTimestamp", $"overall".as("rating"))
      .as[ProductReview]
  }

  def extractProductCategories(metadata: Dataset[Metadata])(implicit spark: SparkSession): Dataset[ProductCategory] = {
    import spark.implicits._

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

    val productReviewsWithCategory: DataFrame = productReviews.join(productCategories, Seq("asin"))
    val categoriesMonth: DataFrame = productReviewsWithCategory.groupBy(
      year($"reviewTimestamp").as("year"),
      month($"reviewTimestamp").as("month"),
      $"category"
    ).agg(
      countDistinct("asin").as("total_sold_products"),
      avg($"rating").as("average_rating")
    )
    val w = Window.partitionBy($"year", $"month").orderBy(desc("total_sold_products"))
    categoriesMonth
      .withColumn("rank", rank.over(w))
      .where($"rank" <= topN)
      .drop("rank")
      .as[CategoryRating]

  }

  def extract(config: CmdConfig)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val reviews: Dataset[Review] = spark.read.parquet(s"${config.sourceDir}/reviews_dedup").as[Review]

    val metadata: Dataset[Metadata] = spark.read.parquet(s"${config.sourceDir}/metadata_dedup").as[Metadata]

    val productReviews: Dataset[ProductReview] = extractProductReviews(reviews)

    val productCategories: Dataset[ProductCategory] = extractProductCategories(metadata)

    val topCategoriesRatings: Dataset[CategoryRating] = computeTopCategoryRatings(productReviews, productCategories)

    topCategoriesRatings
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.targetDir}")
  }

}
