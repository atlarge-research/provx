package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.utils.{GraphUtils, TimeUtils}
import benchmark.ExperimentParameters.{BFS, PageRank, SSSP, WCC}
import provenance.{ProvenanceContext, ProvenanceGraph}
import provenance.GraphLineage.graphToGraphLineage
import provenance.metrics.JSONSerializer
import provenance.query.ProvenancePredicate
import provenance.storage.{EmptyLocation, HDFSLocation, HDFSStorageHandler, TextFile}

import mainargs.{arg, main, ParserForClass}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object Benchmark {
  import utils.CustomCLIArguments._

  @main
  case class Config(
      @arg(name = "config", doc = "Graphalytics benchmark configuration")
      description: ExperimentDescription
  )

  def run(spark: SparkSession, args: Config): (Unit, Long) = TimeUtils.timed {
    val description = args.description
    args.description.benchmarkConfig.debug()
    println(s"Run number   : ${description.runNr}")
    println(s"Experiment ID: ${description.experimentID}")

    val metricsLocation = description.outputDir / "metrics.json"

    val pathPrefix =
      s"${description.benchmarkConfig.datasetPath}/${description.dataset}"
    val (g, config) = GraphUtils.load(spark.sparkContext, pathPrefix)
    val gl = g.withLineage()

    // Create lineage directory for experiment before running it
    val lineagePath = new Path(description.lineageDir)
    val fs = lineagePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(lineagePath)

    ProvenanceContext.setStorageHandler(
      new HDFSStorageHandler(description.lineageDir, format = TextFile(true))
    )

    if (args.description.lineageActive) {
      ProvenanceContext.storageStatus.enable()
    } else {
      ProvenanceContext.storageStatus.disable()
    }

    val filteredGL = gl.capture(
      provenanceFilter = ProvenancePredicate(
        nodePredicate = ProvenanceGraph.allNodes,
        edgePredicate = ProvenanceGraph.allEdges
      )
    )

    val (sol, elapsedTime) = TimeUtils.timed {
      args.description.algorithm match {
        case BFS() => filteredGL.bfs(config.bfsSourceVertex())
        case PageRank() =>
          filteredGL.pageRank(numIter = config.pageRankIterations())
        case SSSP() => filteredGL.sssp(config.ssspSourceVertex())
        case WCC()  => filteredGL.wcc()
        // case "lcc" => gl.lcc()
        // case "cdlp" => Some(gl.cdlp())
        case _ => throw new NotImplementedError("unknown graph algorithm")
      }
    }
    println(s"Run took ${TimeUtils.formatNanoseconds(elapsedTime)}")

    val run = ujson.Obj(
      "duration" -> ujson.Obj(
        "amount" -> ujson.Num(elapsedTime.toDouble),
        "unit" -> "ns"
      )
    )

    if (description.lineageActive) {
      run("iterations") = JSONSerializer.serialize(sol.metrics)
      run("lineageDirectory") = description.lineageDir
    }

    val resultsPath =
      s"${description.benchmarkConfig.outputPath}/experiment-${description.experimentID}/vertices.txt"
    filteredGL.vertices.saveAsTextFile(resultsPath)
    val resultsSize = fileSize(spark, resultsPath)

    val results = ujson.Obj(
      "metrics" -> run
    )

    os.makeDir(description.outputDir)
    os.write(metricsLocation, results)

    val sizes = ProvenanceContext.graph.graph.nodes
      .map((n: ProvenanceGraph.Type#NodeT) => {
        val size = n.outer.g.storageLocation match {
          case Some(loc) =>
            loc match {
              case EmptyLocation() => 0L
              case HDFSLocation(path) =>
                fileSize(spark, path)
              case _ =>
                throw new NotImplementedError("unknown location descriptor")
            }
          case None => 0L
        }

        ujson.Obj(
          "graphID" -> n.outer.g.id,
          "size" -> size.toInt,
          "metrics" -> JSONSerializer.serialize(n.outer.g.metrics)
        )
      })

    val provenance = ujson.Obj(
      "inputs" -> ujson.Obj(
        "config" -> GraphUtils.configPath(pathPrefix),
        "vertices" -> GraphUtils.verticesPath(pathPrefix),
        "edges" -> GraphUtils.edgesPath(pathPrefix),
        "parameters" -> ujson.Obj(
          "applicationId" -> spark.sparkContext.applicationId,
          "algorithm" -> AlgorithmSerializer.serialize(description.algorithm),
          "graph" -> description.dataset,
          "provenance" -> description.lineageActive,
          "runNr" -> description.runNr
        )
      ),
      "output" -> ujson.Obj(
        "metrics" -> metricsLocation.toString,
        "stdout" -> (description.outputDir / "stdout.log").toString,
        "stderr" -> (description.outputDir / "stderr.log").toString,
        "results" -> resultsPath,
        "sizes" -> ujson.Arr(sizes),
        "outputSize" -> resultsSize
      )
    )

    os.write(
      description.outputDir / "graph.dot",
      ProvenanceContext.graph.toDot()
    )

    os.write(
      description.outputDir / "provgraph.json",
      ProvenanceContext.graph.toJson()
    )

    os.write(description.outputDir / "provenance.json", provenance)

    // Clean up lineage folder after being done with it
//    fs.delete(lineagePath, true)
  }

  def fileSize(sparkSession: SparkSession, path: String): Long = {
    val p = new Path(path)
    val fs = p.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val summary = fs.getContentSummary(p)
    summary.getLength
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = ParserForClass[Config].constructOrExit(args.toIndexedSeq)
    val description = parsedArgs.description
    val spark = SparkSession
      .builder()
      .appName(
        s"ProvX ${description.algorithm}/${description.dataset}/${description.lineageActive}/${description.runNr} benchmark"
      )
      .getOrCreate()

    val (_, elapsedTime) = run(spark, parsedArgs)
    println(s"Benchmark run took ${TimeUtils.formatNanoseconds(elapsedTime)}")
  }
}
