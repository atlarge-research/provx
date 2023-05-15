package lu.magalhaes.gilles.provxlib

import utils.{BenchmarkConfig, GraphalyticsConfiguration}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.launcher.SparkLauncher

import java.io.File

object BenchmarkRunner {

  var currentApp: Option[Process] = None

  def main(args: Array[String]): Any = {
    require(args.length >= 1, "Args required: <config>")

    val configPath = args(0)
    val benchmarkConfig = new BenchmarkConfig(configPath)

    val datasetPathPrefix = benchmarkConfig.datasetPath.get
    val metricsPathPrefix = benchmarkConfig.metricsPath.get
    val lineagePathPrefix = benchmarkConfig.lineagePath.get
    val repetitions = benchmarkConfig.repetitions.get
    val benchmarkAlgorithms = benchmarkConfig.algorithms.get.toSet
    val outputPath = benchmarkConfig.outputPath.get

    println(s"Dataset path: ${datasetPathPrefix}")
    println(s"Metrics path: ${metricsPathPrefix}")
    println(s"Lineage path: ${lineagePathPrefix}")
    println(s"Output  path: ${outputPath}")
    println(s"Reptitions: ${repetitions}")
    println(s"Algorithms: ${benchmarkAlgorithms.mkString(", ")}")

    val totalStartTime = System.currentTimeMillis()

    val lineageEnabled = List(true, false)
    val runs = Range.inclusive(1, repetitions).toList

    val t = new Thread(() => {
      val configurations = benchmarkConfig.graphs.get
        .map(dataset => {
          val config = new GraphalyticsConfiguration(
            new Configuration(),
            s"${datasetPathPrefix}/${dataset}.properties"
          )
          (dataset, config)
        })
        .flatMap(c => c._2.algorithms().get.toSet.intersect(benchmarkAlgorithms)
          .toList
          // Skip CDLP and LCC as they are broken as of now
          .flatMap(algorithm => lineageEnabled.map(v => (algorithm, v)))
          .flatMap(v => runs.map(r => (c._1, v._1, v._2, r)))
        )

      for ((dataset, algorithm, withLineage, runNr) <- configurations) {
        println("---")
        println(s"algorithm: ${algorithm}, graph: ${dataset}, lineage: ${withLineage}, run: ${runNr}")

        val cwd = System.getProperty("user.dir")
        println(s"PWD: ${cwd}")

        val lineageOption = if (withLineage) "lineage" else "no-lineage"
        val app = new SparkLauncher()
          .directory(new File(cwd))
          .setAppResource("provxlib-assembly-0.1.0-SNAPSHOT.jar")
          .setMainClass("lu.magalhaes.gilles.provxlib.Benchmark")
          .addAppArgs(configPath, algorithm, dataset, lineageOption, runNr.toString)
          .redirectError(ProcessBuilder.Redirect.INHERIT)
          .redirectOutput(ProcessBuilder.Redirect.INHERIT)
          .setSparkHome("/home/gmo520/bin/spark-3.2.2-bin-hadoop3.2")
          .setVerbose(true)
          .launch()
        currentApp = Some(app)

        app.waitFor()
        if (app.exitValue() != 0) {
          println(s"Error occured: exit code ${app.exitValue()}")
          return 1
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
