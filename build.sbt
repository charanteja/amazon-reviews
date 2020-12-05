name := "amazon-reviews"

version := "1.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-hive" % "3.0.1" % "provided",
  "com.github.scopt" %% "scopt" % "4.0.0",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.0" % Runtime
)

assemblyJarName in assembly := "amazon-reviews-fat.jar"

test in assembly := {}

packageOptions in assembly ~= { pos =>
  pos.filterNot { po =>
    po.isInstanceOf[Package.MainClass]
  }
}