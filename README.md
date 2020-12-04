# amazon-reviews
Repository contains POC concepts for downloading and processing Amazon Reviews Data(http://jmcauley.ucsd.edu/data/amazon/links.html)

### Prerequisities
* Install sbt
* Install Hadoop
* Install Spark

### Building the project
To build the project, please run:
```
sbt assembly
```

It will create a fat jar inside `target` folder

### Submit Spark Jobs
You can submit spark jobs using this syntax

For example, to run spark job:
```
   bin/spark-submit \
     --class org.amazon.reviews.load.LoadReviews \
     --master local[*] \
     --deploy-mode client \
     --conf spark.sql.streaming.schemaInference=true \
     --conf spark.sql.streaming.checkpointLocation=hdfs://localhost:9000/data/checkpoint \
     target/scala-2.12/amazon-reviews-fat.jar \
     --sourceDir hdfs://localhost:9000/<source_directory> \
     --targetDir hdfs://localhost:9000/<target_directory>
```

### Building Docker Image

### Deploy in Kubernetes

### Analysis
To do quick analysis of data, we can start a new zeppelin server(https://zeppelin.apache.org/)

`bin/zeppelin-daemon.sh start`

Link to sample notebook: <zeppelin notebook link>

### References
1. Project on spark structured streaming concept:
https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
2. Argo was used for pipeline orchestration: https://argoproj.github.io/argo/