#!/usr/bin/env bash

bin/spark-submit \
  --class org.amazon.reviews.load.LoadReviews \
  --master local[\*] \
  --driver-memory 16g \
  --deploy-mode client \
  --conf spark.sql.streaming.schemaInference=true \
  --conf spark.sql.streaming.checkpointLocation=hdfs://hadoop-hadoop-hdfs-nn:9000/data/checkpoint \
  amazon-reviews-fat.jar \
  --sourceDir hdfs://hadoop-hadoop-hdfs-nn:9000/data/reviews \
  --targetDir hdfs://hadoop-hadoop-hdfs-nn:9000/data/parquet/reviews