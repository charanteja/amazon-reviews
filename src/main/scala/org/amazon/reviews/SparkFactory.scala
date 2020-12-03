package org.amazon.reviews

import org.apache.spark.sql.SparkSession

object SparkFactory {

  def createSparkSession(sparkAppName: String): SparkSession = {

    SparkSession
      .builder()
      .appName(sparkAppName)
      .enableHiveSupport()
      .getOrCreate()
  }

  def stopSparkSession(sparkSession: SparkSession): Unit = {
    sparkSession.stop()
  }
}
