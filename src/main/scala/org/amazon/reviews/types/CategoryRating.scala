package org.amazon.reviews.types

case class CategoryRating(year: String,
                          month: String,
                          category: String,
                          total_sold_products: Long,
                          average_rating: Double)

