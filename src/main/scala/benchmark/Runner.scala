package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.{BenchmarkConfig, GraphalyticsConfiguration, NotificationsConfig}
import benchmark.utils._

import mainargs.{arg, main, ParserForClass}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.launcher.SparkLauncher

import java.io.File
import java.util.UUID

object Runner {
  import utils.CustomCLIArguments._

  @main
  case class Config(
      @arg(name = "config", doc = "Graphalytics benchmark configuration")
      benchmarkConfig: BenchmarkConfig,
      @arg(name = "description", doc = "Experiment description")
      description: String = ""
  )

  def run(args: Config): Long = {
    if (!args.benchmarkConfig.validate()) {
      println("Invalid configuration")
      return 1
    }

    // Create current experiment directory
    val currentExperimentPath =
      args.benchmarkConfig.createCurrentExperimentDir()
    if (currentExperimentPath.isEmpty) {
      println("Aborting benchmark. Experiments directory exists.")
      return 1
    }

    val experimentPath = currentExperimentPath.get

    // Get experiment description from user via readline (if not specified at CLI)
    val descriptionFilePath = experimentPath / "description.txt"
    FileUtils.writeFile(descriptionFilePath.toString(), Seq(args.description))

    // Copy benchmark config for reproducibility
    os.copy(
      os.Path(args.benchmarkConfig.getPath),
      experimentPath / "config.properties"
    )

    args.benchmarkConfig.debug()

    val sparkHome = sys.env.get("SPARK_HOME")
    if (sparkHome.isEmpty) {
      println("SPARK_HOME env variable must be defined.")
      return 1
    }

    val configurations =
      generateConfigurations(experimentPath, args.benchmarkConfig)

    println(s"Configurations count: ${configurations.length}")

    for (experiment <- Seq(configurations.head)) {
      os.makeDir.all(experiment.outputDir)

      println(
        Console.GREEN +
          (Seq(
            s"Experiment ${experiment.experimentID}:",
            s"algorithm=${experiment.algorithm}",
            s"graph=${experiment.dataset}",
            s"lineage=${experiment.lineageActive}",
            s"run=${experiment.runNr}"
          ) mkString " ") + Console.RESET
      )

      val directory = new File(System.getProperty("user.dir"))
      val outputFile = new File((experiment.outputDir / "stdout.log").toString)
      val errorFile = new File((experiment.outputDir / "stderr.log").toString)

      val (app, elapsedTime) = TimeUtils.timed {
        val appArgs = Array(
          "--config",
          ExperimentDescriptionSerializer.serialize(experiment)
        )

        println("Running configuration:")
        println(appArgs(1))

        val app = new SparkLauncher()
          .directory(directory)
          .setAppResource(args.benchmarkConfig.benchmarkJar)
          .setMainClass("lu.magalhaes.gilles.provxlib.benchmark.Benchmark")
          .addAppArgs(appArgs: _*)
          .redirectOutput(outputFile)
          .redirectError(errorFile)
          .setSparkHome(sparkHome.get)
          .setConf("spark.eventLog.enabled", "true")
          .setConf("spark.eventLog.dir", args.benchmarkConfig.sparkLogs)
          .setVerbose(true)
          .launch()

        app.waitFor()

        app
      }

      if (app.exitValue() != 0) {
        println(s"Error occurred: exit code ${app.exitValue()}")
        return 1
      } else {
        // Empty SUCCESS file indicates that experiment terminated successfully
        os.write(experiment.outputDir / "SUCCESS", "")

        val lineageStatus = if (experiment.lineageActive) 1 else 0
        PushoverNotifier.notify(
          new NotificationsConfig(args.benchmarkConfig.getPath),
          s"ProvX bench: ${experiment.algorithm}/${experiment.dataset}/${lineageStatus}/${experiment.runNr}",
          f"Took ${TimeUtils.formatNanoseconds(elapsedTime)}"
        )
      }
    }

    // Empty SUCCESS file indicates that ALL experiments terminated successfully
    os.write(currentExperimentPath.get / "SUCCESS", "")

    0
  }

  def generateConfigurations(
      outputDir: os.Path,
      benchmarkConfig: BenchmarkConfig
  ): Array[ExperimentDescription] = {
    val lineageEnabled = List(true, false)
    val runs = Range.inclusive(1, benchmarkConfig.repetitions).toList

    benchmarkConfig.graphs
      .flatMap(dataset => {
        new GraphalyticsConfiguration(
          new Configuration(),
          s"${benchmarkConfig.datasetPath}/${dataset}.properties"
        )
          .algorithms()
          .get
          .toSet
          .intersect(benchmarkConfig.algorithms.toSet)
          .map(algorithm => (dataset, algorithm))
      })
      .flatMap(v => lineageEnabled.map(l => (v._1, v._2, l)))
      .flatMap(v => runs.map(r => (v._1, v._2, v._3, r)))
      .map(v => (v._1, v._2, v._3, v._4, UUID.randomUUID()))
      .map(v =>
        ExperimentDescription(
          experimentID = v._5.toString,
          dataset = v._1,
          algorithm = AlgorithmSerializer.deserialize(v._2.toUpperCase),
          lineageActive = v._3,
          runNr = v._4,
          outputDir = outputDir / s"experiment-${v._5}",
          benchmarkConfig = benchmarkConfig,
          lineageDir = benchmarkConfig.lineagePath + s"/experiment-${v._5}"
        )
      )
  }

  def main(args: Array[String]): Unit = {
    val (_, elapsedTime) = TimeUtils.timed {
      run(ParserForClass[Config].constructOrExit(args))
    }

    println(f"Benchmark took ${TimeUtils.formatNanoseconds(elapsedTime)}")
  }
}
