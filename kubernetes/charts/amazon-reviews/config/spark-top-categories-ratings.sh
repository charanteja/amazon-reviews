#!/usr/bin/env bash

bin/spark-submit \
  --class org.amazon.reviews.transform.TopCategoriesRatings \
  --master local[\*] \
  --driver-memory 16g \
  --deploy-mode client \
  --conf spark.sql.streaming.schemaInference=true \
  --conf spark.sql.streaming.checkpointLocation=hdfs://hadoop-hadoop-hdfs-nn:9000/data/checkpoint \
  amazon-reviews-fat.jar \
  --sourceDir hdfs://hadoop-hadoop-hdfs-nn:9000/data/parquet \
  --targetDir hdfs://hadoop-hadoop-hdfs-nn:9000/data/parquet/category_ratings