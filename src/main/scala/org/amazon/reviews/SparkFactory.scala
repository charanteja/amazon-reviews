package org.amazon.reviews

import org.apache.spark.sql.SparkSession

object SparkFactory {

  lazy val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("Amazon Reviews Session")
      .enableHiveSupport()
      .getOrCreate()

  def stopSparkSession(): Unit = {
    sparkSession.stop()
  }
}
