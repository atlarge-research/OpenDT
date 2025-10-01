#bin/bash

sbt package
$SPARK_HOME/bin/spark-submit \
  --class com.example.Main \
  target/scala-2.12/my-spark-job_2.12-0.1.0.jar

