package org.amazon.reviews.types

case class Related(also_bought: Seq[String], also_viewed: Seq[String], bought_together: Seq[String])

case class Metadata(asin: String,
                    title: String,
                    price: Double,
                    imUrl: String,
                    related: Related,
                    brand: String,
                    categories: Seq[Seq[String]])
