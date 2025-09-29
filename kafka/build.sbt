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
    "org.apache.spark" %% "spark-sql-kafka-0-10" % spark % "provided",
    "io.circe" %% "circe-core"    % "0.14.14" % "provided",
    "io.circe" %% "circe-generic" % "0.14.14" % "provided",
    "io.circe" %% "circe-parser"  % "0.14.14" % "provided",
    "com.databricks" %% "databricks-dbutils-scala" % "0.1.4" % "provided"
)

scalacOptions += "-language:postfixOps"

Compile / mainClass := Some("org.opendt.TelemetrySim")