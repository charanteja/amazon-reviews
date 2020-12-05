package org.amazon.reviews.transform

import java.sql.Timestamp.from
import java.time.Instant.ofEpochMilli

import org.amazon.reviews.SparkSessionTestWrapper
import org.amazon.reviews.types.{ProductReview, Review}
import org.apache.spark.sql.Dataset
import org.scalatest.{FunSpec, Matchers}

class TopCategoriesRatingsSpec extends FunSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  describe("Top Category Ratings Spec") {

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
        reviewTimestamp = from(ofEpochMilli(1252800000))
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
        reviewTimestamp = from(ofEpochMilli(1252800000))
      )

      val reviews: Dataset[Review] = Seq(
        review1, review2
      ).toDF.as[Review]

      val expectedProductReviews: Set[ProductReview] = Seq(
        ProductReview(asin = "asin1", rating = 5.0, reviewTimestamp = from(ofEpochMilli(1252800000))),
        ProductReview(asin = "asin2", rating = 4.0, reviewTimestamp = from(ofEpochMilli(1252800000)))
      ).toSet

      val actualProductReviews: Dataset[ProductReview] = TopCategoriesRatings.extractProductReviews(reviews)(spark)

      actualProductReviews.collect().toSet shouldEqual expectedProductReviews
    }
  }

}
