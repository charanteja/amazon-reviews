#!/usr/bin/env bash

# download reviews
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Amazon_Instant_Video.json.gz

# put reviews into hdfs
./hadoop-2.9.0/bin/hadoop fs -copyFromLocal reviews_Amazon_Instant_Video.json.gz hdfs://hadoop-hadoop-hdfs-nn:9000/data/reviews/reviews_Amazon_Instant_Video.json.gz

# download metadata
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Amazon_Instant_Video.json.gz

# put metadata into hdfs
./hadoop-2.9.0/bin/hadoop fs -copyFromLocal meta_Amazon_Instant_Video.json.gz hdfs://hadoop-hadoop-hdfs-nn:9000/data/metadata/meta_Amazon_Instant_Video.json.gz

