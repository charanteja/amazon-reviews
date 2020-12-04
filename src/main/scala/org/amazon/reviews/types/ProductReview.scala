package org.amazon.reviews.types

import java.sql.Timestamp

case class ProductReview(asin: String, rating: Double, reviewTimestamp: Timestamp)

