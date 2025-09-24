package opendt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

import java.sql.Timestamp
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.language.postfixOps

object DigitalTwin {

    private val WINDOW_DUR = s"${Config.WINDOW_SIZE_MS + 1} milliseconds"
    private val SLIDE_DUR  = s"2000 milliseconds"

    private def createKafkaReadStream[T: Encoder](spark: SparkSession, topic: String, idFields: String*): Dataset[T] = {
        val schema = implicitly[Encoder[T]].schema
        val idSchema     = StructType(schema.fields.filter(f => idFields.contains(f.name)))
        val valueSchema  = StructType(schema.fields.filterNot(f => idFields.contains(f.name)))

        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", Config.BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
            .selectExpr("CAST(key AS STRING) AS json_key", "CAST(value AS STRING) AS json_val")
            .select(from_json(col("json_key"), idSchema).as("id"), from_json(col("json_val"), valueSchema).as("data"))
            .select(col("id.*"), col("data.*"))
            .as[T]
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("OpenDT DT")
            .master("local[*]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        Logger.getLogger("org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
            .setLevel(Level.ERROR)

        import spark.implicits._

        val tasks = createKafkaReadStream[Task](spark, Config.TASKS_TOPIC, "id")
            .as("t")

        val fragments = createKafkaReadStream[OpenDTFragment](spark, Config.FRAGMENTS_TOPIC, "id")
            .as("f")

        var i = 0;

        tasks
            .join(fragments, tasks("id") === fragments("id"), "inner")
            .select(
                struct(col("t.*")).as("t"),
                struct(col("f.*")).as("f"),
                col("f.submission_time").as("event_time") // choose the time you want to window on
            )
            .withWatermark("event_time", SLIDE_DUR)
            .groupBy(
                window(col("event_time"), WINDOW_DUR)
            )
            .agg(collect_list(struct(col("t"), col("f"))).as("rows"))
            .writeStream
            .trigger(Trigger.ProcessingTime(Config.WINDOW_SIZE_MS_SIM))
            .foreachBatch { (batch: DataFrame, _: Long) =>
                println(s"Processing batch $i")
                batch
                    .select(col("window.start").as("start"), col("window.end").as("end"))
                    .orderBy(col("start"), col("end"))
                    .distinct()
                    .collect()
                    .foreach { w =>
                        val wStart = w.getAs[Timestamp]("start")
                        val wEnd = w.getAs[Timestamp]("end")

                        println(s"processing ${wStart}-${wEnd}")

                        val records = batch
                            .where(col("window.start") === wStart && col("window.end") === wEnd)
                            .select(explode(col("rows")).as("row"))
                            .select(col("row.*"))

                        val fmt = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm_ss").withZone(ZoneOffset.UTC)
                        val winStartStr = fmt.format(wStart.toInstant)
                        val winEndStr   = fmt.format(wEnd.toInstant)
                        val base = s"opendt_sim_inputs/${winStartStr}-${winEndStr}"

                        val currFragments = records
                            .select(col("f.*"))
                            .withColumn("submission_time", from_utc_timestamp(col("submission_time"), "UTC"))
                            .as[OpenDTFragment]

                        currFragments
                            .write
                            .mode("overwrite")
                            .parquet(s"${base}/fragments")

                        currFragments.groupBy("submission_time").count().orderBy("submission_time").show(100)

                        val tasks = records
                            .select(col("t.*"))
                            .withColumn(
                                "submission_time",
                                when(col("submission_time") > wStart, col("submission_time")).otherwise(wStart)
                            )
                            .withColumn("submission_time", from_utc_timestamp(col("submission_time"), "UTC"))
                            .as[Task]

                        tasks.show(10)

                        tasks
                            .write
                            .mode("overwrite")
                            .parquet(s"${base}/tasks")

                        println(s"record ${wStart}-${wEnd} written")
                    }
                println(s"Processing batch $i finished")
                i += 1
            }
            .start()
            .awaitTermination()

        spark.stop()
    }
}
