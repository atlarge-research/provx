package lu.magalhaes.gilles.provxlib

import utils.{BenchmarkConfig, GraphalyticsConfiguration, NotificationsConfig, PushoverNotifier, TimeUtils}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.launcher.SparkLauncher

import java.io.File
import java.util.UUID

object BenchmarkRunner {

  var currentApp: Option[Process] = None

  def main(args: Array[String]): Any = {
    require(args.length >= 1, "Args required: <config>")

    val configPath = args(0)
    val benchmarkConfig = new BenchmarkConfig(configPath)
    val notificationsConfig = new NotificationsConfig(configPath)

    // inputs
    val datasetPathPrefix = benchmarkConfig.datasetPath.get
    val repetitions = benchmarkConfig.repetitions.get
    val benchmarkAlgorithms = benchmarkConfig.algorithms.get.toSet
    val experimentsPath = os.Path(benchmarkConfig.experimentsPath.get)
    val lineagePathPrefix = benchmarkConfig.lineagePath.get
    val outputPath = benchmarkConfig.outputPath.get
    val sparkLogsDir = benchmarkConfig.sparkLogs.get

    if (os.exists(experimentsPath)) {
      println("Aborting benchmark. Experiments directory exists.")
      return 1
    }

    println(s"Dataset     path: ${datasetPathPrefix}")
    println(s"Lineage     path: ${lineagePathPrefix}")
    println(s"Output      path: ${outputPath}")
    println(s"Experiments path: ${experimentsPath}")
    println(s"Repetitions: ${repetitions}")
    println(s"Algorithms: ${benchmarkAlgorithms.mkString(", ")}")

    val totalStartTime = System.currentTimeMillis()

    val lineageEnabled = List(true, false)
    val runs = Range.inclusive(1, repetitions).toList

    val t = new Thread(() => {
      val configurations = benchmarkConfig.graphs.get
        .flatMap(dataset => {
          val config = new GraphalyticsConfiguration(
            new Configuration(),
            s"${datasetPathPrefix}/${dataset}.properties"
          )
          val allowedAlgorithms = config
            .algorithms()
            .get.toSet
            .intersect(benchmarkAlgorithms)

          allowedAlgorithms
            .map(algorithm => (dataset, algorithm, UUID.randomUUID()))
        })
        .flatMap(v => lineageEnabled.map(l => (v._1, v._2, v._3, l)))
        .flatMap(v => runs.map(r => (v._1, v._2, v._3, v._4, r)))

      for ((dataset, algorithm, experimentID, withLineage, runNr) <- configurations) {

        val expDir = experimentsPath / s"experiment-${experimentID}" / s"run-${runNr}"/ s"lineage-${withLineage.toString}"
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
          .setConf("spark.eventLog.dir", sparkLogsDir)
          .setVerbose(true)
          .launch()
        currentApp = Some(app)

        app.waitFor()
        if (app.exitValue() != 0) {
          println(s"Error occured: exit code ${app.exitValue()}")
          return 1
        } else {
          val runTime = System.nanoTime() - startTime
          val lineageStatus = if (withLineage) 1 else 0
          PushoverNotifier.notify(
            notificationsConfig,
            s"ProvX bench: ${algorithm}/${dataset}/${lineageStatus}/${runNr}",
            f"Took ${TimeUtils.formatNanoseconds(runTime)}"
          )
        }
      }
    })

    sys.addShutdownHook({
      println("Shutting down...")
      if (currentApp.isDefined) {
        println("Attempting shutdown")
        currentApp.get.destroyForcibly()
      }
      val totalEndTime = System.currentTimeMillis()
      val elapsedTime = totalEndTime - totalStartTime
      println(f"Benchmark took ${elapsedTime / 1e3}%.2fs")
      Thread.sleep(1000)
    })

    t.start()
  }
}
