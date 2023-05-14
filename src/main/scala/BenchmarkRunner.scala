package lu.magalhaes.gilles.provxlib

import utils.{BenchmarkConfig, GraphalyticsConfiguration}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.launcher.SparkLauncher

import java.io.File
import scala.util.control.Breaks.{break, breakable}

object BenchmarkRunner {

  var currentApp: Option[Process] = None

  def main(args: Array[String]) = {
    require(args.length >= 1, "Args required: <config>")

    val configPath = args(0)
    val benchmarkConfig = new BenchmarkConfig(configPath)

    val datasetPathPrefix = benchmarkConfig.datasetPath.get
    val metricsPathPrefix = benchmarkConfig.metricsPath.get
    val lineagePathPrefix = benchmarkConfig.lineagePath.get
    val outputPath = benchmarkConfig.outputPath.get

    println(s"Dataset path: ${datasetPathPrefix}")
    println(s"Metrics path: ${metricsPathPrefix}")
    println(s"Lineage path: ${lineagePathPrefix}")
    println(s"Output  path: ${outputPath}")

    val totalStartTime = System.nanoTime()

    val t = new Thread(() => {

      breakable {
        for (dataset <- benchmarkConfig.graphs.get) {
          val config = new GraphalyticsConfiguration(new Configuration(), s"${datasetPathPrefix}/${dataset}.properties")

          for (algorithm <- config.algorithms().get) {
            for (withLineage <- List(true, false)) {
              println("---")
              println(s"algorithm: ${algorithm}, graph: ${dataset}, lineage: ${withLineage}")

              val cwd = System.getProperty("user.dir")
              println(s"PWD: ${cwd}")

              val lineageOption = if (withLineage) "lineage" else "no-lineage"
              val app = new SparkLauncher()
                .directory(new File(cwd))
                .setAppResource("provxlib-assembly-0.1.0-SNAPSHOT.jar")
                .setMainClass("lu.magalhaes.gilles.provxlib.Benchmark")
                .addAppArgs(configPath, algorithm, dataset, lineageOption)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .setSparkHome("/home/gmo520/bin/spark-3.2.2-bin-hadoop3.2")
                .setVerbose(true)
                .launch()
              currentApp = Some(app)

              app.waitFor()
              if (app.exitValue() != 0) {
                println(s"Error occured: exit code ${app.exitValue()}")
                break
              }

            }
          }
        }
      }

    })

    sys.addShutdownHook({
      println("Shutting down...")
      if (currentApp.isDefined) {
        println("Attempting shutdown")
        currentApp.get.destroyForcibly()
      }
      val totalEndTime = System.nanoTime()
      val elapsedTime = totalEndTime - totalStartTime
      println(f"Benchmark took ${elapsedTime / 10e9}%.2fs")
      Thread.sleep(1000)
    })

    t.start()
  }
}
