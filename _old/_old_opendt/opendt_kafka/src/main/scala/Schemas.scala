package opendt

import org.apache.spark.sql.types._

object Schemas {
  val tasksSchema: StructType = StructType(Array(
    StructField("id", IntegerType, false),
    StructField("submission_time", TimestampType, false),
    StructField("duration", LongType, false),
    StructField("cpu_count", IntegerType, false),
    StructField("memory", DoubleType, true)
  ))

  val fragmentSchema: StructType = StructType(Array(
    StructField("id", IntegerType, false),
    StructField("duration", LongType, false),
    StructField("cpu_usage", DoubleType, true),
    StructField("memory_usage", DoubleType, true)
  ))
}
