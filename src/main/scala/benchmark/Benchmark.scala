package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.utils.{GraphUtils, TimeUtils}
import lineage.GraphLineage.graphToGraphLineage
import lineage.LineageLocalContext
import lu.magalhaes.gilles.provxlib.benchmark.configuration.{BenchmarkConfig, GraphalyticsConfiguration}

import mainargs.{arg, main, Flag, ParserForClass}
import org.apache.spark.sql.SparkSession

object Benchmark {
  import utils.CustomCLIArguments._

  @main
  case class Config(@arg(name = "config", doc = "Graphalytics benchmark configuration")
                    benchmarkConfig: BenchmarkConfig,
                    @arg(name = "algorithm", doc = "Algorithm to run")
                    algorithm: String,
                    @arg(name = "dataset", doc = "Dataset to run algorithm on")
                    dataset: String,
                    @arg(name = "lineage", doc = "Lineage enabled")
                    lineageActive: Flag,
                    @arg(name = "runNr", doc = "Run number")
                    runNr: Long,
                    @arg(name = "outputDir", doc = "Directory to store results (local filesystem)")
                    outputDir: os.Path,
                    @arg(name = "experimentID", doc = "Experiment identifier")
                    experimentID: String)


  def run(args: Config): (Unit, Long) = TimeUtils.timed {
    args.benchmarkConfig.debug()
    println(s"Run number   : ${args.runNr}")
    println(s"Experiment ID: ${args.experimentID}")

    val metricsLocation = args.outputDir / "metrics.json"

    val spark = SparkSession.builder
      .appName(s"ProvX ${args.algorithm}/${args.dataset}/${args.lineageActive}/${args.runNr} benchmark")
      .getOrCreate()

    val pathPrefix = s"${args.benchmarkConfig.datasetPath}/${args.dataset}"
    val (g, config) = GraphUtils.load(spark.sparkContext, pathPrefix)
    val gl = g.withLineage()

    gl.lineageContext.getStorageHandler.setLineageDir(args.benchmarkConfig.lineagePath)
    if (args.lineageActive.value) {
      gl.lineageContext.enableTracing()
    } else {
      gl.lineageContext.disableTracing()
    }


    val (sol, elapsedTime) = TimeUtils.timed {
      args.algorithm match {
        case "bfs" => gl.bfs(config.bfsSourceVertex())
        case "wcc" => gl.wcc()
        case "pr" => gl.pageRank(numIter = config.pageRankIterations())
        case "sssp" => gl.sssp(config.ssspSourceVertex())
        // case "lcc" => gl.lcc()
        // case "cdlp" => Some(gl.cdlp())
      }
    }
    println(s"Run took ${TimeUtils.formatNanoseconds(elapsedTime)}")

    val run = ujson.Obj(
      "duration" -> ujson.Obj(
        "amount" -> ujson.Num(elapsedTime),
        "unit" -> "ns"
      )
    )

    if (args.lineageActive.value) {
      run("iterations") = sol.getMetrics().serialize()
      // TODO(gm): include lineage directory in HDFS
      // run("lineageDirectory") = metrics.getLineageDirectory()
    }

    val resultsPath = s"${args.benchmarkConfig.outputPath}/experiment-${args.experimentID}/vertices.txt"
    g.vertices.saveAsTextFile(resultsPath)

    val results = ujson.Obj(
      "metrics" -> run
    )

    os.write(metricsLocation, results)

    val provenance = ujson.Obj(
      "inputs" -> ujson.Obj(
        "config" -> GraphUtils.configPath(pathPrefix),
        "vertices" -> GraphUtils.verticesPath(pathPrefix),
        "edges" -> GraphUtils.edgesPath(pathPrefix),
        "parameters" -> ujson.Obj(
          "applicationId" -> spark.sparkContext.applicationId,
          "algorithm" -> args.algorithm,
          "graph" -> args.dataset,
          "lineage" -> args.lineageActive.value,
          "runNr" -> args.runNr,
        )
      ),
      "output" -> ujson.Obj(
        "metrics" -> metricsLocation.toString,
        "stdout" -> (args.outputDir / "stdout.log").toString,
        "stderr" -> (args.outputDir / "stderr.log").toString,
        "results" -> resultsPath
      )
    )

    os.write(args.outputDir / "provenance.json", provenance)
  }

  def main(args: Array[String]): Unit = {
    val (_, elapsedTime) = run(ParserForClass[Config].constructOrExit(args))
    println(s"Benchmark run took ${TimeUtils.formatNanoseconds(elapsedTime)}")
  }
}