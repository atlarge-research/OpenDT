package opendt

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import java.util.Properties

object TelemetrySimulator {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("OpenDT-TelemetrySimulator")
      .master("local[*]")
      .getOrCreate()

    val tasksPath = args(0)
    val fragmentsPath = args(1)

    // Load data
    val tasks = spark.read.parquet(tasksPath)
    val fragments = spark.read.parquet(fragmentsPath)

    // Setup Kafka producer
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    println("🚀 Starting telemetry simulation...")

    // Stream data to Kafka
    tasks.collect().foreach { row =>
      val key = s"""{"id": ${row.getAs[Int]("id")}}"""
      val value = s"""{"duration": ${row.getAs[Long]("duration")}, "cpu_count": ${row.getAs[Int]("cpu_count")}}"""
      producer.send(new ProducerRecord("tasks", key, value))
    }

    producer.flush()
    producer.close()
    spark.stop()

    println("✅ Telemetry simulation complete")
  }
}
