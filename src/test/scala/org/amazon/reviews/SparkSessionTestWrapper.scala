package org.amazon.reviews

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Amazon Reviews Test Session")
      .getOrCreate()
  }
}
