package opendt

import java.sql.Timestamp

// Data models for Spark Datasets
case class Task(
  id: Int,
  submission_time: Timestamp,
  duration: Long,
  cpu_count: Int,
  memory: Double
)

case class Fragment(
  id: Int,
  duration: Long,
  cpu_usage: Double,
  memory_usage: Double
)

case class OpenDTFragment(
  id: Int,
  submission_time: Timestamp,
  duration: Long,
  cpu_usage: Double,
  memory_usage: Double
)
