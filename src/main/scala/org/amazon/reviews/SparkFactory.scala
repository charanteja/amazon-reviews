package org.amazon.reviews

import org.apache.spark.sql.SparkSession

object SparkFactory {

  lazy val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("Spark App")
      .enableHiveSupport()
      .getOrCreate()

  def stopSparkSession(): Unit = {
    sparkSession.stop()
  }
}
