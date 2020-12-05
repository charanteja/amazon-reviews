package org.amazon.reviews.transform

import java.sql.Timestamp

import org.amazon.reviews.SparkSessionTestWrapper
import org.amazon.reviews.types._
import org.apache.spark.sql.Dataset
import org.scalatest.{FunSpec, Matchers}

class DeduplicateDataSpec extends FunSpec with Matchers with SparkSessionTestWrapper {

  import spark.implicits._

  describe("Deduplicate data as expected") {

    it("should deduplicate reviews data") {

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

      val review3: Review = Review(
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

      val reviews: Dataset[Review] = Seq(
        review1, review2, review3
      ).toDF.as[Review]

      val expectedReviews: Set[Review] = Seq(
        review1, review2
      ).toSet

      val actualReviews: Dataset[Review] = DeduplicateData.deduplicate[Review](reviews)(spark)

      actualReviews.collect().toSet shouldEqual expectedReviews
    }

    it("should deduplicate metadata data") {

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

      val metadata3: Metadata = Metadata(
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
        metadata1, metadata2, metadata3
      ).toDF.as[Metadata]

      val expectedMetadata: Set[Metadata] = Seq(
        metadata1, metadata2
      ).toSet

      val actualMetadata: Dataset[Metadata] = DeduplicateData.deduplicate[Metadata](metadata)(spark)

      actualMetadata.collect().toSet shouldEqual expectedMetadata
    }
  }
}
