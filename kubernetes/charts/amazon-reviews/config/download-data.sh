#!/usr/bin/env bash

# download reviews from the location
wget https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz

# put reviews into hdfs
./hadoop-2.9.0/bin/hadoop fs -copyFromLocal reviews_Amazon_Instant_Video.json.gz hdfs://hadoop-hadoop-hdfs-nn:9000/data/reviews/reviews_Amazon_Instant_Video.json.gz

# download metadata from the location
wget https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz

# put metadata into hdfs
./hadoop-2.9.0/bin/hadoop fs -copyFromLocal meta_Amazon_Instant_Video.json.gz hdfs://hadoop-hadoop-hdfs-nn:9000/data/metadata/meta_Amazon_Instant_Video.json.gz

