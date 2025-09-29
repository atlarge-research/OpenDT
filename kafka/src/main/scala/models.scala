package opendt

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType, TimestampType}

import java.sql.Timestamp
import java.time.LocalDateTime

case class Task(id: Int, submission_time: LocalDateTime, duration: Long,
                cpu_count: Int, cpu_capacity: Double, mem_capacity: Long)

case class Fragment(id: Int, duration: Long, cpu_count: Int, cpu_usage: Double)
case class OpenDTFragment(id: Int, duration: Long, cpu_count: Int, cpu_usage: Double, submission_time: LocalDateTime)

object Schemas {
    val tasksSchema: StructType = new StructType()
        .add("id", IntegerType, nullable = false)
        .add("submission_time", TimestampType, nullable = false)
        .add("duration", LongType, nullable = false)
        .add("cpu_count", IntegerType, nullable = false)
        .add("cpu_capacity", DoubleType, nullable = false)
        .add("mem_capacity", LongType, nullable = false)

    val fragmentSchema: StructType = new StructType()
        .add("id", IntegerType, nullable = false)
        .add("duration", LongType, nullable = false)
        .add("cpu_count", IntegerType, nullable = false)
        .add("cpu_usage", DoubleType, nullable = false)

    val openDTFragmentSchema: StructType = fragmentSchema.add("submission_time", TimestampType, nullable = false)
}