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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.spark.launcher.SparkLauncher

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit

object Runner {
  import utils.CustomCLIArguments._

  type ExperimentTuple = (String, String, String, Int)

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
      dryRun: Boolean = false,
      @arg(
        name = "resume",
        doc = "Resume experiment execution based on existing results"
      )
      resume: Option[String] = None
  )

  def start(args: Config): List[ExperimentTuple] = {
    val benchmarkConfig = args.benchmarkConfig
    val currentExperimentPath = benchmarkConfig.createCurrentExperimentDir()
    if (currentExperimentPath.isEmpty) {
      println("Aborting benchmark. Experiments directory exists.")
      System.exit(1)
    }
    val experimentPath = currentExperimentPath.get

    // Copy dataset properties file data config to `configs` folder in experiment folder
    val configsPath = experimentPath / "configs"
    os.makeDir.all(configsPath)

    benchmarkConfig.graphs.foreach(d => {
      val hdfsPath = new Path(s"${benchmarkConfig.datasetPath}/${d}.properties")
      val localPath =
        new Path(s"${configsPath.toString()}/${d}.properties")
      val fs = hdfsPath.getFileSystem(new Configuration())
      fs.copyToLocalFile(hdfsPath, localPath)
    })

    // Get experiment description from user via readline (if not specified at CLI)
    val descriptionFilePath = experimentPath / "description.txt"
    FileUtils.writeFile(
      descriptionFilePath.toString(),
      Seq(args.description)
    )

    // Copy benchmark config for reproducibility (when running new benchmark)
    os.copy(
      os.Path(benchmarkConfig.getPath),
      experimentPath / "config.properties"
    )

    generateConfigurations(benchmarkConfig).toList
  }

  def run(args: Config, experimentTuples: List[ExperimentTuple]): Long = {
    val benchmarkConfig = args.benchmarkConfig
    val experimentPath = args.benchmarkConfig.currentExperimentDir.get

    println(s"Experiment path: ${experimentPath}")

    val configurations = experimentTuples
      .map(v => {
        val experimentID = UUID.randomUUID()
        ExperimentDescription(
          experimentID = experimentID.toString,
          dataset = v._1,
          algorithm = AlgorithmSerializer.deserialize(v._2.toUpperCase),
          setup = v._3,
          runNr = v._4,
          outputDir = experimentPath / s"experiment-${experimentID}",
          benchmarkConfig = benchmarkConfig,
          lineageDir =
            benchmarkConfig.lineagePath + s"/experiment-${experimentID}"
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
        ExperimentDescriptionSerializer.serialize(experiment).toString
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
            .setAppResource(benchmarkConfig.benchmarkJar)
            .setMainClass("lu.magalhaes.gilles.provxlib.benchmark.Benchmark")
            .addAppArgs(appArgs: _*)
            .redirectOutput(outputFile)
            .redirectError(errorFile)
            .setSparkHome(sparkHome.get)
            .setConf("spark.eventLog.enabled", "true")
            .setConf("spark.eventLog.dir", benchmarkConfig.sparkLogs)
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
      os.write(experimentPath / "SUCCESS", "")
    }

    0
  }

  def resume(args: Config): List[ExperimentTuple] = {
    val benchmarkConfig = args.benchmarkConfig
    println(s"Resume flag: ${args.resume.isDefined}")
    println(s"Loaded configuration from ${args.resume.get}")

    val allConfigurations = generateConfigurations(benchmarkConfig)

    val resultsDir = os.Path(
      "/" + os
        .Path(args.resume.get)
        .segments
        .toList
        .dropRight(1)
        .mkString("/")
    )
    args.benchmarkConfig.currentExperimentDir = Some(resultsDir)

    val experimentDirectories =
      os.list(resultsDir).filter(os.stat(_).fileType == os.FileType.Dir)

    val successfulConfigurations = experimentDirectories
      .filter(os.list(_).map(_.last).contains("SUCCESS"))
      .map(g => {
        val file = ujson.read(os.read(g / "provenance.json"))
        val params = file("inputs")("parameters")

        (
          params("dataset").str,
          params("algorithm").str.toLowerCase,
          params("setup").str,
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
          config(0).replace("()", ""), // algorithm
          config(2).toLowerCase(), // configuration
          config(3).toInt
        )
      })

    val remainingConfigurations: Set[(String, String, String, Int)] =
      allConfigurations.toSet &~ (successfulConfigurations.toSet ++ failedConfigurations.toSet)

    remainingConfigurations.toList.sortWith((t1, t2) => t1._3 < t2._3)
  }

  def startupChecks(benchmarkConfig: BenchmarkConfig): Long = {
    val sparkHome = sys.env.get("SPARK_HOME")
    if (sparkHome.isEmpty) {
      println("SPARK_HOME env variable must be defined.")
      return 1
    }

    if (!benchmarkConfig.validate()) {
      println("Invalid configuration")
      return 1
    }

    benchmarkConfig.debug()

    0
  }

  def generateConfigurations(
      benchmarkConfig: BenchmarkConfig
  ): Array[ExperimentTuple] = {
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
        // Make baseline be the first to execute
        ExperimentSetup.values.toList
          .map(v => v.toString)
          .sorted
          .map(es => (v._1, v._2, es))
//        Seq((v._1, v._2, "Baseline"))
      })
      .flatMap(v => runs.map(r => (v._1, v._2, v._3, r)))
      .sortWith((lhs, rhs) => lhs._3 < rhs._3)
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = ParserForClass[Config].constructOrExit(args)

    val (_, elapsedTime) = TimeUtils.timed {
      val newArgs = if (parsedArgs.resume.isDefined) {
        val resumePath = os.Path(
          parsedArgs.benchmarkConfig.experimentsPath
        ) / parsedArgs.resume.get / "config.properties"

        Config(
          new BenchmarkConfig(s"file://${resumePath.toString()}"),
          parsedArgs.description,
          parsedArgs.dryRun,
          Some(resumePath.toString())
        )
      } else {
        parsedArgs
      }

      if (startupChecks(newArgs.benchmarkConfig) != 0) {
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
