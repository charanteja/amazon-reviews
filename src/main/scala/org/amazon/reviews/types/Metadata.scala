package org.amazon.reviews.types

case class Metadata(asin: String,
                    title: String,
                    price: Double,
                    imUrl: String,
                    related: MetadataRelated,
                    brand: String,
                    categories: Seq[Seq[String]])
