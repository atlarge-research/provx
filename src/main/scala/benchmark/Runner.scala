package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.utils._

import lu.magalhaes.gilles.provxlib.benchmark.configuration.{BenchmarkConfig, GraphalyticsConfiguration, NotificationsConfig}
import mainargs.{arg, main, ParserForClass}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.launcher.SparkLauncher

import java.io.File
import java.util.UUID

object Runner {
  import utils.CustomCLIArguments._

  @main
  case class Config(@arg(name = "config", doc = "Graphalytics benchmark configuration")
                    benchmarkConfig: BenchmarkConfig,
                    @arg(name = "description", doc = "Experiment description")
                    description: String = "")


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

    val sparkHome = sys.env.get("SPARK_HOME")
    if (sparkHome.isEmpty) {
      println("SPARK_HOME env variable must be defined.")
      return 1
    }

    val configurations = generateConfigurations(args.benchmarkConfig)

    println(s"Configurations count: ${configurations.length}")

    for ((dataset, algorithm, withLineage, runNr, experimentID) <- configurations) {

      val expDir = currentExperimentPath.get / s"experiment-${experimentID}"

      os.makeDir.all(expDir)

      println(s"Experiment ${experimentID}: algorithm=${algorithm} graph=${dataset} lineage=${withLineage} run=${runNr}")

      val directory = new File(System.getProperty("user.dir"))
      val outputFile = new File((expDir / "stdout.log").toString)
      val errorFile = new File((expDir / "stderr.log").toString)

      val (app, elapsedTime) = TimeUtils.timed {
        val requiredArgs = Array(
          "--config", args.benchmarkConfig.getPath,
          "--algorithm", algorithm,
          "--dataset", dataset,
          "--runNr", runNr.toString,
          "--outputDir", expDir.toString,
          "--experimentID", experimentID.toString
        )
        val appArgs = if (withLineage) {
          requiredArgs ++ Array("--lineage")
        } else {
          requiredArgs
        }

        val app = new SparkLauncher()
          .directory(directory)
          .setAppResource(args.benchmarkConfig.benchmarkJar)
          .setMainClass("lu.magalhaes.gilles.provxlib.benchmark.Benchmark")
          .addAppArgs(appArgs:_*)
          .redirectOutput(outputFile)
          .redirectError(errorFile)
          .setSparkHome(sparkHome.get)
          .setConf("spark.eventLog.enabled", "true")
          .setConf("spark.eventLog.dir", args.benchmarkConfig.sparkLogs)
          .setConf("spark.driver.memory", "8G")
          .setConf("spark.executor.memory", "8G")
          .setVerbose(true)
          .launch()

        app.waitFor()

        app
      }

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

  def generateConfigurations(benchmarkConfig: BenchmarkConfig): Array[(String, String, Boolean, Int, UUID)] = {
    val lineageEnabled = List(true, false)
    val runs = Range.inclusive(1, benchmarkConfig.repetitions).toList

    benchmarkConfig.graphs
      .flatMap(dataset => {
        new GraphalyticsConfiguration(
          new Configuration(),
          s"${benchmarkConfig.datasetPath}/${dataset}.properties"
        )
          .algorithms()
          .get.toSet
          .intersect(benchmarkConfig.algorithms.toSet)
          .map(algorithm => (dataset, algorithm))
      })
      .flatMap(v => lineageEnabled.map(l => (v._1, v._2, l)))
      .flatMap(v => runs.map(r => (v._1, v._2, v._3, r)))
      .map(v => (v._1, v._2, v._3, v._4, UUID.randomUUID()))
  }

  def main(args: Array[String]): Unit = {
    val (_, elapsedTime) = TimeUtils.timed {
      run(ParserForClass[Config].constructOrExit(args))
    }

    println(f"Benchmark took ${TimeUtils.formatNanoseconds(elapsedTime)}")
  }
}
