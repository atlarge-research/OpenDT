name := "opendt-kafka"
version := "0.1.0"
scalaVersion := "2.12.15"  // Changed from 2.12.17

val sparkVersion = "3.4.0"  // Changed version
val kafkaVersion = "3.3.1"  // Changed version

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion
)

// Simplified assembly config
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
