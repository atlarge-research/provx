package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.{
  BenchmarkConfig,
  GraphalyticsConfiguration,
  NotificationsConfig
}
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
      description: String = "",
      @arg(
        name = "dry-run",
        doc = "Print experiment configurations that will be executed"
      )
      dryRun: Boolean = false
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

    val numConfigurations = configurations.length

    for ((experiment, idx) <- configurations.view.zipWithIndex) {
      os.makeDir.all(experiment.outputDir)

      println(
        Console.BLUE +
          (Seq(
            s"Experiment ${idx + 1}/${numConfigurations}:",
            s"id=${experiment.experimentID}",
            s"algorithm=${experiment.algorithm}",
            s"graph=${experiment.dataset}",
            s"lineage=${experiment.lineageEnabled}",
            s"compression=${experiment.compressionEnabled}",
            s"run=${experiment.runNr}"
          ) mkString " ") + Console.RESET
      )

      val directory = new File(System.getProperty("user.dir"))
      val outputFile = new File((experiment.outputDir / "stdout.log").toString)
      val errorFile = new File((experiment.outputDir / "stderr.log").toString)

      val appArgs = Array(
        "--config",
        ExperimentDescriptionSerializer.serialize(experiment)
      )

      println("Running configuration:")
      println(appArgs(1))

      if (!args.dryRun) {
        val (app, elapsedTime) = TimeUtils.timed {
          val launcher = new SparkLauncher()
            .directory(directory)
            .setAppResource(args.benchmarkConfig.benchmarkJar)
            .setMainClass("lu.magalhaes.gilles.provxlib.benchmark.Benchmark")
            .addAppArgs(appArgs: _*)
            .redirectOutput(outputFile)
            .redirectError(errorFile)
            .setSparkHome(sparkHome.get)
            .setConf("spark.eventLog.enabled", "true")
            .setConf("spark.eventLog.dir", args.benchmarkConfig.sparkLogs)
            .setConf("spark.ui.prometheus.enabled", "true")
            .setConf("spark.eventLog.logStageExecutorMetrics", "true")
            .setConf("spark.metrics.executorMetricsSource.enabled", "true")
//            .setConf("spark.driver.memory", "8G")
//            .setConf("spark.executor.memory", "8G")
            .setVerbose(true)
            .launch()

          // TODO: Launch in another thread and timeout thread if more than 30 minutes
          launcher.waitFor()
          launcher
        }

        if (app.exitValue() != 0) {
          println(s"Error occurred: exit code ${app.exitValue()}")
          return 1
        } else {
          // Empty SUCCESS file indicates that experiment terminated successfully
          os.write(experiment.outputDir / "SUCCESS", "")

          println(
            Console.GREEN + f"Took ${TimeUtils.formatNanoseconds(elapsedTime)}" + Console.RESET
          )
          val lineageStatus = if (experiment.lineageEnabled) 1 else 0
          PushoverNotifier.notify(
            new NotificationsConfig(args.benchmarkConfig.getPath),
            s"ProvX bench: ${experiment.algorithm}/${experiment.dataset}/${lineageStatus}/${experiment.runNr}",
            f"Took ${TimeUtils.formatNanoseconds(elapsedTime)}"
          )
        }
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
    val tracingEnabled = List(true, false)
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
      .flatMap(v => {
        tracingEnabled.flatMap(l => {
          if (l) {
            // If lineage is enabled, run experiment with compression turned on/off
            Seq(
              // Storage on, compression on
              (v._1, v._2, true, true, true),
              // Storage on, compression off
              (v._1, v._2, true, true, false),
              // Storage off, comopression off
              (v._1, v._2, true, false, false)
            )
          } else {
            // If tracing is disabled, just run one experiment
            Seq((v._1, v._2, false, false, false))
          }
        })
      })
      .flatMap(v => runs.map(r => (v._1, v._2, v._3, v._4, v._5, r)))
      .map(v => {
        val experimentID = UUID.randomUUID()
        ExperimentDescription(
          experimentID = experimentID.toString,
          dataset = v._1,
          algorithm = AlgorithmSerializer.deserialize(v._2.toUpperCase),
          lineageEnabled = v._3,
          storageEnabled = v._4,
          compressionEnabled = v._5,
          runNr = v._6,
          outputDir = outputDir / s"experiment-${experimentID}",
          benchmarkConfig = benchmarkConfig,
          lineageDir =
            benchmarkConfig.lineagePath + s"/experiment-${experimentID}"
        )
      })
  }

  def main(args: Array[String]): Unit = {
    val (_, elapsedTime) = TimeUtils.timed {
      run(ParserForClass[Config].constructOrExit(args))
    }

    println(f"Benchmark took ${TimeUtils.formatNanoseconds(elapsedTime)}")
  }
}
