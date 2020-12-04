FROM openjdk:8-jre
MAINTAINER Charan Kattumenu

RUN      apt-get update && \
         apt-get install -y curl && \
         wget https://archive.apache.org/dist/hadoop/common/hadoop-2.9.0/hadoop-2.9.0.tar.gz && \
         tar zxf hadoop-2.9.0.tar.gz && rm -rf hadoop-2.9.0.tar.gz

COPY     target/*/amazon-reviews-fat.jar /
