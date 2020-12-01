name := "amazon-reviews"

version := "1.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-streaming" % "3.0.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.1",
  "org.apache.spark" %% "spark-hive" % "3.0.1",
  "com.github.scopt" %% "scopt" % "4.0.0"
)
