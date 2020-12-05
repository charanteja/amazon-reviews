package org.amazon.reviews.transform

import java.sql.Timestamp

import org.amazon.reviews.SparkSessionTestWrapper
import org.amazon.reviews.types._
import org.apache.spark.sql.Dataset
import org.scalatest.{FunSpec, Matchers}

class TopCategoriesRatingsSpec extends FunSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  describe("Extract data from source") {

    it("should extract product reviews from reviews") {

      val review1: Review = Review(
        reviewerID = "reviewer1",
        asin = "asin1",
        reviewerName = "x",
        helpful = Seq(4, 5),
        reviewText = "random text",
        overall = 5.0,
        summary = "random summary",
        unixReviewTime = 1252800000,
        reviewTime = "09 13, 2009",
        reviewTimestamp = Timestamp.valueOf("2009-09-13 00:00:00")
      )

      val review2: Review = Review(
        reviewerID = "reviewer2",
        asin = "asin2",
        reviewerName = "y",
        helpful = Seq(3, 4),
        reviewText = "random text",
        overall = 4.0,
        summary = "random summary",
        unixReviewTime = 1252800000,
        reviewTime = "09 13, 2009",
        reviewTimestamp = Timestamp.valueOf("2009-09-13 00:00:00")
      )

      val reviews: Dataset[Review] = Seq(
        review1, review2
      ).toDF.as[Review]

      val expectedProductReviews: Set[ProductReview] = Seq(
        ProductReview(asin = "asin1", rating = 5.0, reviewTimestamp = Timestamp.valueOf("2009-09-13 00:00:00")),
        ProductReview(asin = "asin2", rating = 4.0, reviewTimestamp = Timestamp.valueOf("2009-09-13 00:00:00"))
      ).toSet

      val actualProductReviews: Dataset[ProductReview] = TopCategoriesRatings.extractProductReviews(reviews)(spark)

      actualProductReviews.collect().toSet shouldEqual expectedProductReviews
    }

    it("should extract product categories from metadata") {

      val metadata1: Metadata = Metadata(
        asin = "asin1",
        title = "title1",
        price = 10.0,
        imUrl = "http://random-url-1",
        related = MetadataRelated(
          also_bought = Seq.empty[String],
          also_viewed = Seq.empty[String],
          bought_together = Seq.empty[String]
        ),
        brand = "brand1",
        categories = Seq(Seq("category_1", "category_2"))
      )

      val metadata2: Metadata = Metadata(
        asin = "asin2",
        title = "title2",
        price = 20.0,
        imUrl = "http://random-url-2",
        related = MetadataRelated(
          also_bought = Seq.empty[String],
          also_viewed = Seq.empty[String],
          bought_together = Seq.empty[String]
        ),
        brand = "brand2",
        categories = Seq(Seq("category_1", "category_3"))
      )

      val metadata: Dataset[Metadata] = Seq(
        metadata1, metadata2
      ).toDF.as[Metadata]

      val expectedProductCategories: Set[ProductCategory] = Seq(
        ProductCategory(asin = "asin1", category = "category_1"),
        ProductCategory(asin = "asin1", category = "category_2"),
        ProductCategory(asin = "asin2", category = "category_1"),
        ProductCategory(asin = "asin2", category = "category_3")
      ).toSet

      val actualProductReviews: Dataset[ProductCategory] = TopCategoriesRatings
        .extractProductCategories(metadata)(spark)

      actualProductReviews.collect().toSet shouldEqual expectedProductCategories
    }
  }

  describe("Compute expected metrics") {

    it("should compute top product category ratings") {

      val productReviews: Dataset[ProductReview] = Seq(
        ProductReview(asin = "asin1", rating = 5.0, reviewTimestamp = Timestamp.valueOf("2009-09-13 00:00:00")),
        ProductReview(asin = "asin2", rating = 4.0, reviewTimestamp = Timestamp.valueOf("2009-09-13 00:00:00"))
      ).toDF.as[ProductReview]

      val productCategories: Dataset[ProductCategory] = Seq(
        ProductCategory(asin = "asin1", category = "category_1"),
        ProductCategory(asin = "asin1", category = "category_2"),
        ProductCategory(asin = "asin2", category = "category_1"),
        ProductCategory(asin = "asin2", category = "category_3")
      ).toDF.as[ProductCategory]

      val actualCategoryRatings: Dataset[CategoryRating] = TopCategoriesRatings
        .computeTopCategoryRatings(productReviews, productCategories)(spark)

      val expectedCategoryRatings: Set[CategoryRating] = Seq(
        CategoryRating(year = "2009", month = "9", category = "category_1", total_sold_products = 2, average_rating = 4.5),
        CategoryRating(year = "2009", month = "9", category = "category_2", total_sold_products = 1, average_rating = 5.0),
        CategoryRating(year = "2009", month = "9", category = "category_3", total_sold_products = 1, average_rating = 4.0),
      ).toSet

      actualCategoryRatings.collect().toSet shouldEqual expectedCategoryRatings
    }
  }
}
