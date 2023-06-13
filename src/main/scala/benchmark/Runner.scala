package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.utils._

import mainargs.{arg, main, ParserForClass}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.launcher.SparkLauncher

import java.io.File
import java.util.UUID

object Runner {
  import utils.CLIReader._

  @main
  case class Config(@arg(name = "configPath", doc = "Graphalytics benchmark configuration")
                    benchmarkConfig: BenchmarkConfig,
                    @arg(name = "description", doc = "Experiment description")
                    description: String = "")


  var currentApp: Option[Process] = None

  def run(args: Config): Long = {
    if (!args.benchmarkConfig.validate()) {
      println("Invalid configuration")
      return 1
    }

    // Create current experiment directory
    val currentExperimentPath = args.benchmarkConfig.createCurrentExperimentDir()
    if (currentExperimentPath.isEmpty) {
      println("Aborting benchmark. Experiments directory exists.")
      return 1
    }

    // Get experiment description from user via readline (if not specified at CLI)
    val descriptionFilePath = currentExperimentPath.get / "description.txt"
    FileUtils.writeFile(descriptionFilePath.toString(), Seq(args.description))

    // Copy benchmark config for reproducibility
    os.copy(
      os.Path(args.benchmarkConfig.getPath),
      currentExperimentPath.get / "config.properties"
    )

    args.benchmarkConfig.debug()

    val lineageEnabled = List(true, false)
    val runs = Range.inclusive(1, args.benchmarkConfig.repetitions).toList

    val configurations = args.benchmarkConfig.graphs
      .flatMap(dataset => {
        new GraphalyticsConfiguration(
          new Configuration(),
          s"${args.benchmarkConfig.datasetPath}/${dataset}.properties"
        )
        .algorithms()
        .get.toSet
        .intersect(args.benchmarkConfig.algorithms.toSet)
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

      val (app, elapsedTime) = TimeUtils.timed {
        val app = new SparkLauncher()
          .directory(new File(cwd))
          .setAppResource("provxlib-assembly-0.1.0-SNAPSHOT.jar")
          .setMainClass("lu.magalhaes.gilles.provxlib.Benchmark")
          .addAppArgs(
            "--configPath", args.benchmarkConfig.getPath,
            "--algorithm", algorithm,
            "--dataset", dataset,
            if (withLineage) {
              "--lineage"
            } else {
              ""
            },
            "--runNr", runNr.toString,
            "--experimentDir", expDir.toString)
          .redirectOutput(outputFile)
          .redirectError(errorFile)
          .setSparkHome("/home/gmo520/bin/spark-3.2.2-bin-hadoop3.2")
          .setConf("spark.eventLog.enabled", "true")
          .setConf("spark.eventLog.dir", args.benchmarkConfig.sparkLogs)
          .setVerbose(true)
          .launch()

        app.waitFor()

        app
      }

      currentApp = Some(app)

      if (app.exitValue() != 0) {
        println(s"Error occurred: exit code ${app.exitValue()}")
        return 1
      } else {
        val lineageStatus = if (withLineage) 1 else 0
        PushoverNotifier.notify(
          new NotificationsConfig(args.benchmarkConfig.getPath),
          s"ProvX bench: ${algorithm}/${dataset}/${lineageStatus}/${runNr}",
          f"Took ${TimeUtils.formatNanoseconds(elapsedTime)}"
        )
      }
    }

    // Create an empty SUCCESS file to indicate that experiments terminated successfully
    os.write(currentExperimentPath.get / "SUCCESS", "")

    0
  }

  def main(args: Array[String]): Unit = {
    val (_, elapsedTime) = TimeUtils.timed {
      run(ParserForClass[Config].constructOrExit(args))
    }

    sys.addShutdownHook({
      println("Shutting down...")
      currentApp.map { app =>
        println("Attempting shutdown")
        app.destroyForcibly()
      }
      println(f"Benchmark took ${TimeUtils.formatNanoseconds(elapsedTime)}")
      Thread.sleep(1000)
    })
  }
}
