package org.amazon.reviews

import org.amazon.reviews.config.CmdConfig
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import scopt.OParser.parse

trait RunSpark extends Logging {

  def main(args: Array[String]) {
    parse(CmdConfig.parserSpec(), args, CmdConfig()) match {
      case Some(config) =>
        logger.info("Starting Spark Session")
        implicit val spark: SparkSession = SparkFactory.sparkSession
        extract(config)
        SparkFactory.stopSparkSession()
        logger.info("Stopped Spark Session")
      case _ => System.exit(1)
    }
  }

  def extract(config: CmdConfig)(implicit spark: SparkSession): Unit
}
