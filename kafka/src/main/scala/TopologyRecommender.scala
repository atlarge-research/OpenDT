package opendt

import io.circe.generic.auto._
import io.circe.parser.parse
import io.circe.syntax.EncoderOps
import io.circe.{Json, JsonObject}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession, functions}

import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.sys.process._

case class Window(wStart: Timestamp, wEnd: Timestamp, tasks: Dataset[Task], fragments: Dataset[OpenDTFragment])
case class SloInfo(kwh: Double, maxPowerDraw: Float)

class TopologyRecommender(private val spark: SparkSession, private val openDCPath: String, private val experimentSetupTemplate: JsonObject,
                          private val topologyTemplate: JsonObject){

    private var bestTopologyInfo: (JsonObject, SloInfo) = (topologyTemplate, SloInfo(0.0, 0.0f))
    private var candidates: List[(JsonObject, SloInfo)] = List()
    private val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    private val openDcOutputDir: String = "./output/simple/raw-output/0/seed=0"
    private val powerSourceFile: String  = s"$openDcOutputDir/powerSource.parquet"

    private def convertToOpenDCFile(df: DataFrame, workloadDir: String, newFname: String, schema: StructType): Option[FileStatus] = {
        val dfTyped = df.sparkSession.createDataFrame(df.rdd, schema)
            .repartition(1)

        val dest = s"$workloadDir/$newFname"
        dfTyped
            .write
            .mode("overwrite")
            .parquet(dest)

        val dstPath = new Path(dest)
        val maybeFile: Option[FileStatus] =
            fs.listStatus(dstPath).find(st => st.isFile && st.getPath.getName.endsWith("snappy.parquet"))

        maybeFile.map { st =>
            val targetPath = new Path(s"$workloadDir/$newFname.parquet")

            val renamed = fs.rename(st.getPath, targetPath)
            if (!renamed) throw new RuntimeException(s"Failed to rename ${st.getPath} -> $targetPath")

            fs.delete(dstPath, true)
            st
        }
    }

    private def dumpJsonObj(pathStr: String, obj: Json): Unit = {
        val path = new Path(pathStr)
        val parent = path.getParent
        if (parent != null && !fs.exists(parent)) fs.mkdirs(parent)

        val out = fs.create(path, /* overwrite = */ true)
        try {
            out.write(obj.spaces4.getBytes)
            out.hflush()                                                  // optional: push buffers
        } finally {
            out.close()
        }
    }

    private def runCmd(cmd: Seq[String]): (Int, String, String) = {
        val out = new StringBuilder
        val err = new StringBuilder

        // `!` returns the exit code; ProcessLogger captures output lines
        val exit = cmd.!(ProcessLogger(
            (o: String) => { out.append(o).append('\n') },   // stdout
            (e: String) => { err.append(e).append('\n') }    // stderr
        ))

        (exit, out.result(), err.result())
    }

    private def simulate(batch: Iterable[Window], topology: JsonObject): SloInfo = {
        //1. Create the fragments.parquet and tasks.parquet from the files that are associated with th windows provided
        //2. Make sure the merged tasks file is sorted by submission_time, and that the fragments.parquet are also sorted accrodingly how OPenDc expects
        //3. Dump the current topology
        //4. Make the experiment Setup point to the new workload
        //5. call opendc and wait for it to finish
        //6. save the result in a folder that is marked with the lowest wStart and highest wEnd across these windows
        //7. parse the SLO's from these files as needed

        val batchStart = batch.minBy(w => w.wStart).wStart
        val batchEnd = batch.maxBy(w => w.wEnd).wEnd

        val fmt = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm_ss").withZone(ZoneOffset.UTC)
        val winStartStr = fmt.format(batchStart.toInstant)
        val winEndStr   = fmt.format(batchEnd.toInstant)
        val baseDir = s"opendt_sim_inputs/$winStartStr-$winEndStr"
        val workloadDir = s"$baseDir/workload"

        import spark.implicits._

        var openDcTasks = spark.emptyDataset[Task]
        var openDTFragments = spark.emptyDataset[OpenDTFragment]
        batch.foreach { w =>
            openDcTasks = openDcTasks.union(w.tasks)
            openDTFragments = openDTFragments.union(w.fragments)
        }

        val openDcFragments =  openDTFragments
            .drop("submission_type")
            .as[Fragment]
            .toDF()

        convertToOpenDCFile(openDcTasks.toDF(), workloadDir, "tasks", Schemas.tasksSchema)
        convertToOpenDCFile(openDcFragments, workloadDir, "fragments", Schemas.fragmentSchema)

        val topologyPath = s"$workloadDir/topology.json"
        dumpJsonObj(topologyPath, Json.fromJsonObject(topology))

        //make experiment setup point to new topology and workload
        var experimentSetup = experimentSetupTemplate.add("topologies", Json.arr(Json.obj(("pathToFile", Json.fromString(topologyPath)))))
        experimentSetup = experimentSetup.add("workloads", Json.arr(Json.obj(
            ("pathToFile", Json.fromString(workloadDir)),
            ("type", Json.fromString("ComputeWorkload"))
        )))

        val experimentFile = "experiment.json"
        dumpJsonObj(experimentFile, Json.fromJsonObject(experimentSetup))

        val (code, stdout, stderr) = runCmd(Seq(openDCPath, "--experiment-path", experimentFile))
        if (code != 0){
            println(s"stdout: \n$stdout")
            println(s"stderr: \n$stderr")
            throw new Exception(s"Error when running OpenDC got error code $code")
        }

        //derive the stats we need from the OpenDC run and delete the rest
        //save these stats in baseDir
        //fil in SloInfo object
        val powerSourceDf = spark.read
            .parquet(powerSourceFile)

        val energyUsageSum = powerSourceDf
            .agg(functions.sum("energy_usage"))
            .first()
            .getDouble(0)

        val energyUsageKwh = energyUsageSum / 3600000
        val maxPowerDraw = powerSourceDf
            .agg(functions.max("power_draw"))
            .first()
            .getFloat(0)

        val res = SloInfo(energyUsageKwh, maxPowerDraw)

        //cleanup opendc output
        val p = new Path(openDcOutputDir)
        val ok = fs.delete(p, /* recursive = */ true)
        if (!ok && fs.exists(p)) {
            throw new RuntimeException(s"Failed to delete $openDcOutputDir")
        }

        //save slo
        dumpJsonObj(s"$baseDir/slos.json", res.asJson)

        res
    }

    /*
    Finds the best topology and returns the SloInfo of that winning topology.
    Besides that it also returns all the considered candidates along with their slo's for nalaysis by the human in the loop
    */
    private def findBestTopologiesHelper(windows: Iterable[Window]): Unit = {

        //Simulate in a loop and save the topoligies we have along with the SLO's they yield
        //have a timeout set such that this finishes just before the next window will start!
        //so with a window of 5 minutes have a timeout of 4:30 minutes

        //For now we have 1 simulation and we dont change topologies.
        //TODO: integrate Mahesh'es solution here, call his python script

        val sloInfo = simulate(windows, bestTopologyInfo._1)
        bestTopologyInfo = (bestTopologyInfo._1, sloInfo)
    }

    def findBestTopologies(windows: Iterable[Window])
                          (implicit ec: ExecutionContext)

    : (JsonObject, SloInfo, List[(JsonObject, SloInfo)]) = {


        val f = Future { findBestTopologiesHelper(windows) }
        try{
            Await.result(f, Duration(Config.TOPOLOGY_FINDING_DEADLINE_MS, SECONDS))
        }catch {
            case _: concurrent.TimeoutException => ()
        }
        val res = Tuple3(bestTopologyInfo._1, bestTopologyInfo._2, candidates)
        candidates = List()
        res
    }
}

object TopologyRecommender {
    def apply(spark: SparkSession, openDCPath: String, experimentTemplatePath: String,
              topologyTemplatePath: String) : TopologyRecommender = {

        val experimentSetupTemplate: JsonObject =
            parse(Files.readString(Paths.get(experimentTemplatePath))) match {
                case Right(j) => j.asObject.getOrElse(sys.error("experimentSetupTemplate root is not a JSON object"))
                case Left(e)  => sys.error(s"Invalid JSON in experimentSetupTemplate: $e")
            }

        val topologyTemplate: JsonObject =
            parse(Files.readString(Paths.get(topologyTemplatePath))) match {
                case Right(j) => j.asObject.getOrElse(sys.error("topologyTemplate root is not a JSON object"))
                case Left(e)  => sys.error(s"Invalid JSON in topologyTemplate: $e")
            }

        new TopologyRecommender(spark, openDCPath, experimentSetupTemplate, topologyTemplate)
    }
}
