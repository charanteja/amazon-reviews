package org.amazon.reviews.config

object SparkSettings {

  val sqlWarehousedir: String = "hdfs://localhost:9000/user/hive/warehouse"
  val hiveMetastoreUri: String = "thrift://localhost:9083"

}
