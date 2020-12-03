package org.amazon.reviews.extract

import org.amazon.reviews.config.{CmdConfig, SparkSettings}
import org.amazon.reviews.types.Review
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OParser.parse

object ExtractReviews {

  def main(args: Array[String]) {
    parse(CmdConfig.parserSpec(), args, CmdConfig()) match {
      case Some(config) =>
        implicit val spark: SparkSession = SparkSession
          .builder()
          .appName("Extract Reviews")
          .config("spark.sql.warehouse.dir", SparkSettings.sqlWarehousedir)
          .config("hive.metastore.uris", SparkSettings.hiveMetastoreUri)
          .enableHiveSupport()
          .getOrCreate()
        extract(config)
        spark.stop()
      case _ => System.exit(1)
    }
  }

  def extract(config: CmdConfig)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val reviews: Dataset[Review] = spark.read.json(s"${config.source}").as[Review]
    reviews.write.insertInto("records")
  }
}
