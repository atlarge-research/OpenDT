package opendt

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.Timestamp
import java.time.Duration
import java.util.Properties
import scala.util.{Failure, Success, Try}

object TelemetrySim {

  private var traceStart: Timestamp = _
  private var recordsSent: Long = 0

  /**
   * Enhanced Kafka streaming with better error handling and metrics
   */
  private def startStreamingKafka[T](
    topicName: String,
    dataFrame: Dataset[T],
    primaryKeys: String*
  ): Unit = {

    val keyCols = primaryKeys.map(col)
    val valueCols = dataFrame.columns.filterNot(primaryKeys.contains).map(col)

    val transformed = dataFrame.select(
      to_json(struct(keyCols: _*)).as("key"),
      to_json(struct(valueCols: _*)).as("value"),
      col("submission_time")
    ).orderBy("submission_time")

    println(s"🚀 Starting Kafka streaming for topic: $topicName")

    // Process each partition
    transformed.repartition(1).foreachPartition { rows: Iterator[Row] =>

      // Kafka producer configuration
      val props = new Properties()
      props.put("bootstrap.servers", Config.BOOTSTRAP_SERVERS)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("acks", "all")
      props.put("retries", "3")
      props.put("batch.size", "16384")
      props.put("linger.ms", "10")

      val producer = new KafkaProducer[String, String](props)
      var partitionRecords: Long = 0
      var currSt = traceStart

      println(s"📡 Processing partition for topic: $topicName")

      try {
        rows.foreach { row =>
          Try {
            val st: Timestamp = row.getAs("submission_time")

            // Calculate real-time delay based on simulation ratio
            val dur = Duration.between(currSt.toInstant, st.toInstant)
            val actualDur = (dur.toMillis * (Config.SIM_TIME_RATIO / 2)).toLong

            if (actualDur > 0) {
              Thread.sleep(actualDur)
            }
            currSt = st

            // Send to Kafka
            val record = new ProducerRecord(
              topicName,
              row.getAs[String]("key"),
              row.getAs[String]("value")
            )

            val future = producer.send(record)
            partitionRecords += 1

            if (partitionRecords % 1000 == 0) {
              println(s"📊 Sent $partitionRecords records to $topicName")
              producer.flush() // Ensure delivery
            }

          } match {
            case Success(_) => // Record sent successfully
            case Failure(exception) =>
              println(s"❌ Failed to send record to $topicName: ${exception.getMessage}")
          }
        }

      } finally {
        // Cleanup
        producer.flush()
        producer.close()
        recordsSent += partitionRecords
        println(s"✅ Finished streaming partition: $partitionRecords records sent to $topicName")
      }
    }

    println(s"🎉 Streaming complete for $topicName: $recordsSent total records")
  }

  /**
   * Enhanced data loading with schema validation
   */
  private def loadDataWithValidation[T](
    spark: SparkSession,
    file: String,
    schema: StructType,
    datasetName: String
  )(implicit encoder: org.apache.spark.sql.Encoder[T]): Dataset[T] = {

    println(s"📂 Loading $datasetName from: $file")

    Try {
      val dataset = spark.read
        .schema(schema)
        .parquet(file)
        .withColumn("id", col("id").cast("int"))
        .as[T]

      val count = dataset.count()
      println(s"✅ Loaded $datasetName: $count records")
      dataset

    } match {
      case Success(dataset) => dataset
      case Failure(exception) =>
        println(s"❌ Failed to load $datasetName: ${exception.getMessage}")
        throw exception
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: TelemetrySim <tasks_parquet_path> <fragments_parquet_path>")
      System.exit(1)
    }

    val tasksPath = args(0)
    val fragmentsPath = args(1)

    println("🏗️  Initializing OpenDT Telemetry Simulator")

    // Setup Spark
    val spark = SparkSession
      .builder()
      .appName("OpenDT-TelemetrySim")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    try {
      // Load datasets with validation
      val tasks = loadDataWithValidation[Task](
        spark, tasksPath, Schemas.tasksSchema, "Tasks"
      )

      val fragments = loadDataWithValidation[Fragment](
        spark, fragmentsPath, Schemas.fragmentSchema, "Fragments"
      )

      // Calculate fragment timing relative to tasks
      val rollingWindow = Window
        .partitionBy(col("id"))
        .orderBy(col("seq"))
        .rowsBetween(Window.unboundedPreceding, -1)

      println("🔄 Processing fragments with timing calculations...")

      var fragmentsDf = fragments
        .select(col("*"), monotonically_increasing_id().as("surrogate"))
        .withColumn("seq", row_number().over(
          Window.partitionBy(col("id")).orderBy(col("surrogate"))
        ))
        .drop("surrogate")
        .withColumn(
          "durationRelToTask",
          coalesce(sum(col("duration")).over(rollingWindow), lit(0L))
        )
        .drop("seq")

      // Join with tasks to get submission times
      val tasksProj = tasks.select(
        col("id").as("task_id"),
        col("submission_time")
      )

      fragmentsDf = fragmentsDf
        .join(tasksProj, fragments("id") === tasksProj("task_id"))
        .drop("task_id")

      // Create OpenDT fragments with adjusted timing
      val openDTFragments = fragmentsDf
        .withColumn("start_time",
          expr("timestampadd(MILLISECOND, (CAST(duration AS BIGINT) + CAST(durationRelToTask AS BIGINT)), submission_time)")
        )
        .drop("submission_time")
        .withColumnRenamed("start_time", "submission_time")
        .drop("durationRelToTask")
        .as[OpenDTFragment]

      // Get trace start time for simulation timing
      traceStart = tasks
        .select(col("submission_time"))
        .agg(min("submission_time"))
        .first()
        .getTimestamp(0)

      println(s"📅 Simulation starting from: $traceStart")
      println(s"⚡ Simulation speed ratio: ${Config.SIM_TIME_RATIO}")

      // Start streaming threads
      println("🎬 Starting streaming threads...")

      val tasksStream = new Thread(() => {
        startStreamingKafka(Config.TASKS_TOPIC, tasks, "id")
      }, "TasksStreamer")

      val fragmentsStream = new Thread(() => {
        startStreamingKafka(Config.FRAGMENTS_TOPIC, openDTFragments, "id")
      }, "FragmentsStreamer")

      // Start both streams
      tasksStream.start()
      fragmentsStream.start()

      // Wait for completion
      tasksStream.join()
      fragmentsStream.join()

      println("🏁 Telemetry simulation completed successfully!")

    } catch {
      case ex: Exception =>
        println(s"❌ Simulation failed: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
