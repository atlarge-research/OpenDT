package org.opendt

import opendt.{Config, Schemas}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.time.Duration
import java.util.Properties
import scala.collection.mutable.ArrayBuffer


object TelemetrySim {

    private var traceStart: Timestamp = _

    private def startStreamingKafka(topicName: String, dataFrame: DataFrame, primaryKeys: String*): Unit = {
        val keyCols   = primaryKeys.map(col)
        val valueCols = dataFrame.columns.filterNot(primaryKeys.contains).map(col)

        val transformed = dataFrame.select(
            to_json(struct(keyCols: _*)).as("key"),
            to_json(struct(valueCols: _*)).as("value"),
            col("submission_time")
        ).orderBy("submission_time")

        transformed.repartition(1).foreachPartition { rows: Iterator[Row] =>
            println(s"Going over partition for $topicName")
            val props = new Properties()
            props.put("bootstrap.servers", Config.BOOTSTRAP_SERVERS)
            props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

            val producer = new KafkaProducer[String, String](props)
            var currSt = traceStart

            var i = 0L

            //var rows = ArrayBuffer[Row]()
            rows.foreach(row => {
                val st: Timestamp = row.getAs("submission_time")

                val dur = Duration.between(currSt.toInstant, st.toInstant)
                val actualDur = (dur.toMillis * (Config.SIM_TIME_RATIO / 2)).toLong
                if(actualDur > 0) Thread.sleep(actualDur)
                currSt = st

                //send the value
                val _ = producer.send(
                    new ProducerRecord(topicName, row.getAs("key"), row.getAs("value")),
                )
            })

            producer.flush()
            producer.close()
            println("Finished sending")
        }
    }

    def main(args: Array[String]): Unit = {
        val tasks_fname     = args(0)
        val fragments_fname = args(1)
        val spark = SparkSession
            .builder()
            .appName("OpenDT PT")
            .master("local[*]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        val tasks: DataFrame     = spark.read.schema(Schemas.tasksSchema).parquet(tasks_fname)
        var fragments: DataFrame = spark.read.schema(Schemas.fragmentSchema).parquet(fragments_fname)

        fragments  =
            fragments
                .select(col("*"), monotonically_increasing_id().as("surrogate"))
                .withColumn("seq", row_number().over(Window.partitionBy(col("id")).orderBy(col("surrogate"))))
                .drop("surrogate")

        val rollingWindow = Window.partitionBy(col("id")).orderBy(col("seq"))
            .rowsBetween(Window.unboundedPreceding, -1)

        fragments = fragments.withColumn(
            "durationRelToTask",
            coalesce(sum(col("duration")).over(rollingWindow), lit(0L))
        ).drop("seq")

        val tasksProj = tasks.select(
            col("id").as("task_id"),
            col("submission_time")
        )

        fragments = fragments
            .join(tasksProj, fragments("id") === tasksProj("task_id"))
            .drop("task_id")

        fragments = fragments
            .withColumn("start_time", expr("timestampadd(MILLISECOND, (CAST(duration AS BIGINT) + CAST(durationRelToTask AS BIGINT)), submission_time)"))
            .drop("submission_time")
            .withColumnRenamed("start_time", "submission_time")
            .drop("durationRelToTask")

        traceStart = tasks.select(col("submission_time")).agg(min("submission_time")).first().getTimestamp(0)
        val tasksStream = new Thread(() => startStreamingKafka(Config.TASKS_TOPIC, tasks, "id"))
        val fragmentsStream = new Thread(() => startStreamingKafka(Config.FRAGMENTS_TOPIC, fragments, "id"))

        tasksStream.start()
        fragmentsStream.start()

        tasksStream.join()
        fragmentsStream.join()
        spark.stop()
        }
}
