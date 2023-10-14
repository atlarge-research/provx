package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration._
import benchmark.configuration.BenchmarkAppConfig.write
import benchmark.configuration.ExperimentSetup.{ExperimentSetup, StorageFormats}
import benchmark.configuration.GraphAlgorithm.GraphAlgorithm
import benchmark.utils._
import provenance.storage._

import mainargs.{arg, main, ParserForClass}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.launcher.SparkLauncher

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit

object Runner {
  import utils.CustomCLIArgs._

  type ExperimentTuple =
    (String, GraphAlgorithm, ExperimentSetup, StorageFormat, Int)

  @main
  case class Config(
      @arg(name = "config", doc = "Graphalytics benchmark configuration")
      runnerConfig: RunnerConfigData,
      @arg(name = "description", doc = "Experiment description")
      description: String = "",
      @arg(
        name = "dry-run",
        doc = "Print experiment configurations that will be executed"
      )
      dryRun: Boolean = false,
      @arg(
        name = "resume",
        doc = "Resume experiment execution based on existing results"
      )
      resume: Option[String] = None
  )

  def start(args: Config): List[ExperimentTuple] = {
    val runnerConfig = args.runnerConfig.runner
    val expPath = os.Path(runnerConfig.experimentsPath)

    // Copy dataset properties file data config to `configs` folder in experiment folder
    val configsPath = expPath / "configs"
    os.makeDir.all(configsPath)

    // Write configuration files to local storage for benchmark results reproducibility
    runnerConfig.graphs.foreach(d => {
      val hdfsPath = new Path(s"${runnerConfig.datasetPath}/${d}.properties")
      val localPath =
        new Path(s"${configsPath.toString()}/${d}.properties")
      val fs = hdfsPath.getFileSystem(new Configuration())
      fs.copyToLocalFile(hdfsPath, localPath)
    })

    // Get experiment description from user via readline (if not specified at CLI)
    val descriptionFilePath = expPath / "description.txt"
    FileUtils.writeFile(
      descriptionFilePath.toString(),
      Seq(args.description)
    )

    // Copy benchmark config for reproducibility (when running new benchmark)
    os.write(
      expPath / "config.json",
      RunnerConfig.write(args.runnerConfig)
    )

    generateConfigurations(args.runnerConfig)
  }

  def run(args: Config, experimentTuples: List[ExperimentTuple]): Long = {
    val runnerConfig = args.runnerConfig.runner
    val currentExperimentPath =
      os.Path(args.runnerConfig.runner.experimentsPath)

    println(s"Experiment path: ${currentExperimentPath}")

    val configurations = experimentTuples
      .map(v => {
        val experimentID = UUID.randomUUID()
        BenchmarkAppConfig(
          experimentID = experimentID.toString,
          dataset = v._1,
          datasetPath = runnerConfig.datasetPath,
          algorithm = v._2,
          setup = v._3,
          storageFormat = v._4,
          runNr = v._5,
          outputDir = currentExperimentPath / s"experiment-${experimentID}",
          graphalyticsConfigPath =
            s"${runnerConfig.datasetPath}/${v._1}.properties",
          lineageDir = runnerConfig.lineagePath + s"/experiment-${experimentID}"
        )
      })

    println(s"Configurations count: ${configurations.length}")

    val numConfigurations = configurations.length

    for ((experiment, idx) <- configurations.view.zipWithIndex) {
      println(
        Console.BLUE +
          (Seq(
            s"Experiment ${idx + 1}/${numConfigurations}:",
            s"id=${experiment.experimentID}",
            s"algorithm=${experiment.algorithm}",
            s"graph=${experiment.dataset}",
            s"setup=${experiment.setup}",
            s"run=${experiment.runNr}"
          ) mkString " ") + Console.RESET
      )

      val appArgs = Array(
        "--config",
        write(experiment)
      )

      if (!args.dryRun) {
        println("Running configuration:")
        println(appArgs(1))

        os.makeDir.all(experiment.outputDir)

        val directory = new File(System.getProperty("user.dir"))
        val outputFile = new File(
          (experiment.outputDir / "stdout.log").toString
        )
        val errorFile = new File((experiment.outputDir / "stderr.log").toString)
        val sparkHome = sys.env.get("SPARK_HOME")

        val ((app, expectedExit), elapsedTime) = TimeUtils.timed {
          val launcher = new SparkLauncher()
            .directory(directory)
            .setAppResource(runnerConfig.jar)
            .setMainClass("lu.magalhaes.gilles.provxlib.benchmark.Benchmark")
            .addAppArgs(appArgs: _*)
            .redirectOutput(outputFile)
            .redirectError(errorFile)
            .setSparkHome(sparkHome.get)
            .setConf("spark.eventLog.enabled", "true")
            .setConf("spark.eventLog.dir", runnerConfig.sparkLogs)
            .setConf("spark.ui.prometheus.enabled", "true")
            .setConf("spark.eventLog.logStageExecutorMetrics", "true")
            .setConf("spark.metrics.executorMetricsSource.enabled", "true")
//            .setConf("spark.driver.memory", "8G")
//            .setConf("spark.executor.memory", "8G")
            .setVerbose(true)
            .launch()

          os.write(experiment.outputDir / "IN-PROGRESS", "")

          val status = launcher.waitFor(30, TimeUnit.MINUTES)
          (launcher, status)
        }

        if (!expectedExit) {
          NtfyNotifier.notify(
            s"ProvX bench: ${experiment.algorithm}/${experiment.dataset}/${experiment.setup}/${experiment.runNr}",
            s"Failed to finish within 30 minutes deadline.",
            emoji = Some("sos")
          )

          // Empty FAILURE file indicates that experiment failed
          os.write(experiment.outputDir / "FAILURE", "")
        } else {
          if (app.exitValue() != 0) {
            println(s"Error occurred: exit code ${app.exitValue()}")
            return 1
          } else {
            // Empty SUCCESS file indicates that experiment terminated successfully
            os.write(experiment.outputDir / "SUCCESS", "")

            println(
              Console.GREEN + f"Took ${TimeUtils.formatNanoseconds(elapsedTime)}" + Console.RESET
            )
            NtfyNotifier.notify(
              s"ProvX bench: ${experiment.algorithm}/${experiment.dataset}/${experiment.setup}/${experiment.runNr}",
              f"Took ${TimeUtils.formatNanoseconds(elapsedTime)}",
              emoji = Some("hourglass_flowing_sand")
            )
          }
        }

        os.remove(experiment.outputDir / "IN-PROGRESS")
      }
    }

    // Empty SUCCESS file indicates that ALL experiments terminated successfully
    if (args.resume.isEmpty) {
      os.write(currentExperimentPath / "SUCCESS", "")
    }

    0
  }

  def resume(args: Config): List[ExperimentTuple] = {
    val benchmarkConfig = args.runnerConfig
    println(s"Resume flag: ${args.resume.isDefined}")
    println(s"Loaded configuration from ${args.resume.get}")

    val allConfigurations = generateConfigurations(benchmarkConfig)

    val experimentDirectories =
      os.list(os.Path(args.runnerConfig.runner.experimentsPath))
        .filter(os.stat(_).fileType == os.FileType.Dir)

    val successfulConfigurations = experimentDirectories
      .filter(os.list(_).map(_.last).contains("SUCCESS"))
      .map(g => {
        val file = ujson.read(os.read(g / "provenance" / "inputs.json"))
        val params = file("parameters")

        (
          params("dataset").str,
          GraphAlgorithm.withName(params("algorithm").str),
          ExperimentSetup.withName(params("setup").str),
          StorageFormat.fromString(params("storageFormat").str),
          params("runNr").str.toInt
        )
      })
      .toArray

    val failedConfigurations = experimentDirectories
      .filter(os.list(_).map(_.last).contains("FAILURE"))
      .map(g => {
        val stdoutFile = os.read(g / "stdout.log")
        val config = stdoutFile
          .split("\n")
          .filter(_.contains("SparkContext: Submitted application"))(0)
          .split(" ")
          .takeRight(2)(0)
          .split("/")

        (
          config(1), // dataset
          GraphAlgorithm.withName(config(0).replace("()", "")), // algorithm
          ExperimentSetup.withName(config(2)), // configuration
          StorageFormat.fromString(config(3)),
          config(4).toInt
        )
      })

    val remainingConfigurations: Set[ExperimentTuple] =
      allConfigurations.toSet &~ (successfulConfigurations.toSet ++ failedConfigurations.toSet)

    remainingConfigurations.toList
  }

  def startupChecks(runnerConfig: RunnerConfigData): Long = {
    val sparkHome = sys.env.get("SPARK_HOME")

    val conditions = Seq(
      (sparkHome.isEmpty, "SPARK_HOME env variable must be defined."),
      (runnerConfig.runner.setups.isEmpty, "No setups defined."),
      (runnerConfig.runner.graphs.isEmpty, "No graphs to benchmark on."),
      (runnerConfig.runner.algorithms.isEmpty, "No algorithms to benchmark.")
    )

    for ((condition, explanation) <- conditions) {
      if (condition) {
        println(explanation)
        return 1
      }
    }

    0
  }

  def generateConfigurations(
      runnerConfig: RunnerConfigData
  ): List[ExperimentTuple] = {
    val benchmarkConfig = runnerConfig.runner
    val runs = Range.inclusive(1, benchmarkConfig.repetitions).toList

    benchmarkConfig.graphs
      .flatMap(dataset => {
        GraphalyticsConfig
          .loadHadoop(s"${benchmarkConfig.datasetPath}/${dataset}.properties")
          .algorithms
          .intersect(benchmarkConfig.algorithms)
          .map(algorithm => (dataset, algorithm))
      })
      .flatMap(v => {
        benchmarkConfig.setups
          .filterNot(_ == ExperimentSetup.StorageFormats)
          .map(es => (v._1, v._2, es, TextFile())) ++
          Seq(
            TextFile(),
            ObjectFile(),
            ParquetFile(),
            AvroFile(),
            ORCFile(),
            CSVFile(),
            JSONFormat(),
            // Compressible formats
            TextFile(true),
            CSVFile(true),
            JSONFormat(true)
          ).map(fmt => (v._1, v._2, ExperimentSetup.StorageFormats, fmt))
      })
      .flatMap(v => runs.map(r => (v._1, v._2, v._3, v._4, r)))
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = ParserForClass[Config].constructOrExit(args)

    val (_, elapsedTime) = TimeUtils.timed {
      val newArgs = if (parsedArgs.resume.isDefined) {
        // parsedArgs.resume example: "20231011-1010"
        val resultsDir = os.Path(
          parsedArgs.runnerConfig.runner.experimentsPath
        ) / parsedArgs.resume.get
        val runnerConfigPath = resultsDir / "config.properties"

        if (!os.exists(runnerConfigPath)) {
          println("Runner configuration does not exist")
          return
        }

        val resumeConfig =
          RunnerConfig.loadFile(
            s"${runnerConfigPath.toString()}"
          ) match {
            case Left(value) =>
              println(value.toString())
              return
            case Right(value) => value
          }

        val newRunnerConfig =
          resumeConfig.runner.copy(experimentsPath = resultsDir.toString())

        parsedArgs.copy(
          runnerConfig = RunnerConfigData(newRunnerConfig),
          resume = Some(resultsDir.toString())
        )
      } else {
        val currentExperimentPath =
          RunnerConfig.currentExperimentDir(
            parsedArgs.runnerConfig.runner.experimentsPath
          )

        println(currentExperimentPath)

        if (os.exists(currentExperimentPath)) {
          println("Aborting benchmark. Experiments directory exists.")
          System.exit(1)
        }

        os.makeDir.all(currentExperimentPath)

        val updatedRunnerConfig = parsedArgs.runnerConfig.runner
          .copy(experimentsPath = currentExperimentPath.toString())

        RunnerConfigData(updatedRunnerConfig).debug()

        parsedArgs.copy(
          runnerConfig = RunnerConfigData(updatedRunnerConfig)
        )
      }

      if (startupChecks(newArgs.runnerConfig) != 0) {
        println("An error occurred while running startup checks!")
        System.exit(1)
      }

      val experimentTuples = if (parsedArgs.resume.isDefined) {
        resume(newArgs)
      } else {
        start(newArgs)
      }

      run(newArgs, experimentTuples)
    }

    println(f"Benchmark took ${TimeUtils.formatNanoseconds(elapsedTime)}")

    if (!parsedArgs.dryRun) {
      NtfyNotifier.notify(
        "Finished benchmark",
        emoji = Some("white_check_mark")
      )
    }
  }
}
