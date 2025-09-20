package org.opendt

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.sql.Timestamp
import opendt._
import org.apache.spark.sql.expressions.Window

object Main {
    val TASKS_TOPIC    = "tasks"
    val FRAGMENTS_TOPIC = "fragments"

    def main(args: Array[String]): Unit = {
        val tasks_fname     = args(0)
        val fragments_fname = args(1)
        val spark = SparkSession
            .builder()
            .appName("OpenDT PT")
            .master("local[*]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        var tasks: DataFrame     = spark.read.schema(Schemas.tasksSchema).parquet(tasks_fname).repartition(12)
        val fragments: DataFrame = spark.read.schema(Schemas.fragmentSchema).parquet(fragments_fname).repartition(12)

        tasks = tasks.orderBy(col("submission_time").asc)

        var newFragments: DataFrame =
            fragments
                .select(col("*"), monotonically_increasing_id().as("surrogate"))
                .withColumn("seq", row_number().over(Window.partitionBy(col("id")).orderBy(col("surrogate"))))
                .drop("surrogate")

        //create "start_time" field
        val rollingWindow = Window.partitionBy(col("id")).orderBy(col("seq"))
            .rowsBetween(Window.unboundedPreceding, -1)

        newFragments = newFragments.withColumn(
            "durationRelToTask",
            coalesce(sum(col("duration")).over(rollingWindow), lit(0L))
        ).drop("seq")

        val tasksProj = tasks.select(
            col("id").as("task_id"),
            col("submission_time")
        )

        newFragments = newFragments
            .join(tasksProj, newFragments("id") === tasksProj("task_id"))
            .drop("task_id")

        newFragments = newFragments
            .withColumn("start_time", expr("timestampadd(MILLISECOND, durationRelToTask, submission_time)"))
            .drop("submission_type")
            .orderBy(col("start_time"))


        //now we send all the data via kafka
        //for the sake of the demo we simulate 1s to be 300 seconds

        //first convert to kafka format, so key -> value
        val kafkaTasks = tasks.select(
            col("id").as("key"),
            to_json(struct(tasks.columns.map(col): _*)).as("value")
        )

        val kafkaFragments = newFragments.select(
            col("id").as("key"),
            to_json(struct(newFragments.columns.map(col): _*)).as("value")
        )

        val tasksStream = kafkaTasks.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "TODO")
            .option("topic", "opendt-tasks")
            .start()

        val fragmentsStream = kafkaFragments.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "TODO")
            .option("topic", "opendt-fragments")
            .start()

        tasksStream.awaitTermination()
        fragmentsStream.awaitTermination()
        spark.stop()
    }
}
