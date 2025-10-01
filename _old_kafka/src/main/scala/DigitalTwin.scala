package opendt

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

import java.sql.Timestamp
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.language.postfixOps

object DigitalTwin {
    private val WINDOW_DUR = s"${Config.WINDOW_SIZE_MS + 1} milliseconds"
    private val SLIDE_DUR  = s"2000 milliseconds"

    private def createKafkaReadStream[T: Encoder](spark: SparkSession, topic: String, schema: StructType, idFields: String*): Dataset[T] = {
        val idSchema = StructType(schema.fields.filter{f => idFields.contains(f.name)})
        val valueSchema = StructType(schema.fields.filterNot{f => idFields.contains(f.name)})

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

        val openDcPath = args(0)
        val experimentTemplatePath = args(1)
        val topologyTemplatePath = args(2)

        val topologyRecommender: TopologyRecommender =
            TopologyRecommender(spark, openDcPath, experimentTemplatePath, topologyTemplatePath)

        spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")

        spark.sparkContext.setLogLevel("WARN")
        Logger.getLogger("org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
            .setLevel(Level.ERROR)

        val ec: ExecutionContext =
            ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

        import spark.implicits._

        val tasks = createKafkaReadStream[Task](spark, Config.TASKS_TOPIC, Schemas.tasksSchema, "id")
            .as("t")

        val fragments = createKafkaReadStream[OpenDTFragment](spark, Config.FRAGMENTS_TOPIC, Schemas.openDTFragmentSchema, "id")
            .as("f")

        var win = 1;
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

                val windows = ArrayBuffer[Window]()
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

                        val currFragments = records
                            .select(col("f.*"))
                            .withColumn("submission_time", from_utc_timestamp(col("submission_time"), "UTC"))
                            .as[OpenDTFragment]

                        val tasks = records
                            .select(col("t.*"))
                            .dropDuplicates("id")
                            .withColumn(
                                "duration",
                                greatest(lit(0L), col("duration").cast("long") - lit(Config.WINDOW_SIZE_MS * win))
                            )
                            .withColumn("submission_time", from_utc_timestamp(col("submission_time"), "UTC"))
                            .as[Task]
                        win += 1

                        windows += Window(wStart, wEnd, tasks, currFragments)
                    }

                if(windows.nonEmpty){
                    val topologyRes = topologyRecommender.findBestTopologies(windows)(ec)
                    println(s"Best topology in this batch is: ${topologyRes._1}, with the follwoing slo's: ${topologyRes._2}")
                    println("Other candidates ranked from best to worst by Kwh")

                    var rank = 1
                    topologyRes._3
                        .sortBy(topologyInf => topologyInf._2.kwh)
                        .foreach(topologyInf => {

                            println(s"Rank $rank")

                            println("Topology: ")
                            println(topologyInf._1)

                            val sloInf = topologyInf._2
                            println("SLO's: ")
                            println(s"Kwh = ${sloInf.kwh}")
                            println(s"Max power draw = ${sloInf.maxPowerDraw}")
                            rank += 1
                        })
                }
                println(s"Processing batch $i finished")
                i += 1
            }
            .start()
            .awaitTermination()

        spark.stop()
    }
}
