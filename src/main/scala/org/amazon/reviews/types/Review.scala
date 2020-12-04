package org.amazon.reviews.types

import java.sql.Timestamp

case class Review(reviewerID: String,
                  asin: String,
                  reviewerName: String,
                  helpful: Seq[Long],
                  reviewText: String,
                  overall: Double,
                  summary: String,
                  unixReviewTime: Long,
                  reviewTime: String,
                  reviewTimestamp: Timestamp)

