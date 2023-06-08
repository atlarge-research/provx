package lu.magalhaes.gilles.provxlib
package benchmark

import utils._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.launcher.SparkLauncher

import java.io.File
import java.util.UUID
import scala.io.StdIn.readLine

object Runner {

  var currentApp: Option[Process] = None

  def main(args: Array[String]): Any = {
    require(args.length >= 1, "Args required: <config> <experiment description>")

    val configPath = args(0)
    val cliExperimentDescription = args.lift(1)
    val config = new BenchmarkConfig(configPath)

    if (!config.validate()) {
      println("Invalid configuration")
      return 1
    }

    // Create current experiment directory
    val currentExperimentPath = config.createCurrentExperimentDir()
    if (currentExperimentPath.isEmpty) {
      println("Aborting benchmark. Experiments directory exists.")
      return 1
    }

    // Get experiment description from user via readline (if not specified at CLI)
    val descriptionFilePath = currentExperimentPath.get / "description.txt"
    val description = if (cliExperimentDescription.isEmpty) {
      print("Experiment description: ")
      val experimentDescription = readLine()
      experimentDescription
    } else {
      cliExperimentDescription.get
    }
    FileUtils.writeFile(descriptionFilePath.toString(), Seq(description))

    // Copy benchmark config for reproducibility
    os.copy(
      os.Path(configPath),
      currentExperimentPath.get / "config.properties"
    )

    config.debug()

    val totalStartTime = System.nanoTime()

    val lineageEnabled = List(true, false)
    val runs = Range.inclusive(1, config.repetitions).toList

    val t = new Thread(() => {
      val configurations = config.graphs
        .flatMap(dataset => {
          new GraphalyticsConfiguration(
            new Configuration(),
            s"${config.datasetPath}/${dataset}.properties"
          )
          .algorithms()
          .get.toSet
          .intersect(config.algorithms.toSet)
          .map(algorithm => (dataset, algorithm, UUID.randomUUID()))
        })
        .flatMap(v => lineageEnabled.map(l => (v._1, v._2, v._3, l)))
        .flatMap(v => runs.map(r => (v._1, v._2, v._3, v._4, r)))

      println(s"Configurations count: ${configurations.length}")

      for ((dataset, algorithm, experimentID, withLineage, runNr) <- configurations) {

        val expDir = currentExperimentPath.get /
          s"experiment-${experimentID}" /
          s"run-${runNr}"/
          s"lineage-${withLineage.toString}"

        os.makeDir.all(expDir)

        println("---")
        println(s"algorithm: ${algorithm}, graph: ${dataset}, lineage: ${withLineage}, run: ${runNr}")

        val cwd = System.getProperty("user.dir")

        val outputFile = new File((expDir / "stdout.out").toString)
        val errorFile = new File((expDir / "stderr.out").toString)

        val startTime = System.nanoTime()

        val lineageOption = if (withLineage) "lineage" else "no-lineage"
        val app = new SparkLauncher()
          .directory(new File(cwd))
          .setAppResource("provxlib-assembly-0.1.0-SNAPSHOT.jar")
          .setMainClass("lu.magalhaes.gilles.provxlib.Benchmark")
          .addAppArgs(configPath, algorithm, dataset, lineageOption, runNr.toString, expDir.toString)
          .redirectOutput(outputFile)
          .redirectError(errorFile)
          .setSparkHome("/home/gmo520/bin/spark-3.2.2-bin-hadoop3.2")
          .setConf("spark.eventLog.enabled", "true")
          .setConf("spark.eventLog.dir", config.sparkLogs)
          .setVerbose(true)
          .launch()
        currentApp = Some(app)

        app.waitFor()
        if (app.exitValue() != 0) {
          println(s"Error occurred: exit code ${app.exitValue()}")
          return 1
        } else {
          val runTime = System.nanoTime() - startTime
          val lineageStatus = if (withLineage) 1 else 0
          PushoverNotifier.notify(
            new NotificationsConfig(configPath),
            s"ProvX bench: ${algorithm}/${dataset}/${lineageStatus}/${runNr}",
            f"Took ${TimeUtils.formatNanoseconds(runTime)}"
          )
        }
      }

      // Create an empty SUCCESS file to indicate that experiments terminated successfully
      os.write(currentExperimentPath.get / "SUCCESS", "")
    })

    sys.addShutdownHook({
      println("Shutting down...")
      if (currentApp.isDefined) {
        println("Attempting shutdown")
        currentApp.get.destroyForcibly()
      }
      val totalEndTime = System.nanoTime()
      val elapsedTime = totalEndTime - totalStartTime
      println(f"Benchmark took ${TimeUtils.formatNanoseconds(elapsedTime)}")
      Thread.sleep(1000)
    })

    t.start()
  }
}
