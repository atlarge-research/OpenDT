ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

val spark        = "4.0.1"
val scalaBinary  = "2.13"

lazy val devMode = sys.props.get("devMode").contains("true")

lazy val root = (project in file("."))
  .settings(
    name := "untitled",
    idePackagePrefix := Some("opendt")
  )

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % spark % "provided",
    "org.apache.spark" %% "spark-sql"  % spark % "provided",
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.postgresql" % "postgresql" % "42.7.8" % "provided",
    "org.apache.spark" %% "spark-sql-kafka-0-10" % spark % "provided"
)

Compile / mainClass := Some("org.opendt.TelemetrySim")