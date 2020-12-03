package org.amazon.reviews.types

import java.sql.Timestamp

case class Review(reviewerID: String,
                  asin: String,
                  reviewerName: String,
                  helpful: Seq[Int],
                  reviewText: String,
                  overall: Double,
                  summary: String,
                  unixReviewTime: Timestamp,
                  reviewTime: String)

