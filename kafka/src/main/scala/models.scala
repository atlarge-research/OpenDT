package opendt

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType, TimestampType}

import java.sql.Timestamp

case class Task(id: String, submission_time: Timestamp, duration: Long,
                cpu_count: Int, cpu_capacity: Double, mem_capacity: Long)

case class Fragment(id: String, duration: Long, cpu_count: Int, cpu_usage: Double)

case class OpenDTFragment(id: String, duration: Long, cpu_count: Int, cpu_usage: Double, submission_time: Timestamp)

object Schemas {
    val tasksSchema: StructType = new StructType()
        .add("id", StringType, nullable = false)
        .add("submission_time", TimestampType, nullable = false)
        .add("duration", LongType, nullable = false)
        .add("cpu_count", IntegerType, nullable = false)
        .add("cpu_capacity", DoubleType, nullable = false)
        .add("mem_capacity", LongType, nullable = false)

    val fragmentSchema: StructType = new StructType()
        .add("id", StringType, nullable = false)
        .add("duration", LongType, nullable = false)
        .add("cpu_count", IntegerType, nullable = false)
        .add("cpu_usage", DoubleType, nullable = false)

    val newFragemntsSchema: StructType = fragmentSchema.add("start_time", TimestampType, nullable = false)
}
