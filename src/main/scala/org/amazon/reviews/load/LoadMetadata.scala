package org.amazon.reviews.load

import org.amazon.reviews.RunSpark
import org.amazon.reviews.config.CmdConfig
import org.amazon.reviews.types.Metadata
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Spark streaming job to process metadata data
  * Assumes new data comes in a hdfs directory as separate files
  */
object LoadMetadata extends RunSpark {

  def extract(config: CmdConfig)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    logger.info("Reading metadata stream data")
    val metadata: Dataset[Metadata] = spark
      .readStream
      .json(s"${config.sourceDir}")
      .drop("salesRank", "_corrupt_record")
      .as[Metadata]

    logger.info("Writing reviews data in micro batch mode")
    //TODO Optimize window size
    /**
      * Stream is processed in micro-batch mode
      * Window size to be used should be optimized by size and speed of data
      */
    val query: StreamingQuery = metadata
      .writeStream
      .format("parquet")
      .option("path", s"${config.targetDir}")
      .start()

    // Wait for termination signal
    query.awaitTermination()
  }
}
