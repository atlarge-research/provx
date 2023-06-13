package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.utils.{BenchmarkConfig, GraphUtils, TimeUtils}
import lineage.GraphLineage.graphToGraphLineage
import lineage.LineageContext

import mainargs.{arg, main, Flag, ParserForClass}
import org.apache.spark.sql.SparkSession

object Benchmark {
  import utils.CLIReader._

  @main
  case class Config(@arg(name = "configPath", doc = "Graphalytics benchmark configuration")
                    benchmarkConfig: BenchmarkConfig,
                    @arg(name = "algorithm", doc = "Algorithm to run")
                    algorithm: String,
                    @arg(name = "dataset", doc = "Dataset to run algorithm on")
                    dataset: String,
                    @arg(name = "lineage", doc = "Lineage enabled")
                    lineageActive: Flag,
                    @arg(name = "runNr", doc = "Run number")
                    runNr: Long,
                    @arg(name = "experimentDir", doc = "Directory to store results (local filesystem)")
                    experimentDir: os.Path)


  def run(args: Config): Unit = {
    args.benchmarkConfig.debug()
    println(s"Run number: ${args.runNr}")

    val (_, elapsedTime) = TimeUtils.timed {
      val spark = SparkSession.builder
        .appName(s"ProvX ${args.algorithm}/${args.dataset}/${args.lineageActive}/${args.runNr} benchmark")
        .getOrCreate()

      LineageContext.setLineageDir(spark.sparkContext, args.benchmarkConfig.lineagePath)
      if (args.lineageActive.value) {
        LineageContext.enableCheckpointing()
      } else {
        LineageContext.disableCheckpointing()
      }

      val (g, config) = GraphUtils.load(spark.sparkContext, args.benchmarkConfig.datasetPath, args.dataset)
      val gl = g.withLineage()

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

      val postfix = if (args.lineageActive.value) {
        "-lineage"
      } else {
        ""
      }
      g.vertices.saveAsTextFile(s"${args.benchmarkConfig.outputPath}/run-${args.runNr}/${args.algorithm}-${args.dataset}${postfix}.txt")

      val results = ujson.Obj(
        "applicationId" -> spark.sparkContext.applicationId,
        "algorithm" -> args.algorithm,
        "graph" -> args.dataset,
        "lineage" -> args.lineageActive.value,
        "runNr" -> args.runNr,
        "metadata" -> run
      )

      os.write(args.experimentDir / "metrics.json", results)
    }

    println(s"Benchmark run took ${TimeUtils.formatNanoseconds(elapsedTime)}")
  }

  def main(args: Array[String]): Unit = {
    run(ParserForClass[Config].constructOrExit(args))
  }
}