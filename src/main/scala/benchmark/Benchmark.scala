package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.ExperimentParameters.{BFS, PageRank, SSSP, WCC}
import benchmark.utils.{GraphUtils, TimeUtils}
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
    print(description)

    val pathPrefix =
      s"${description.benchmarkConfig.datasetPath}/${description.dataset}"
    val (g, config) = GraphUtils.load(spark.sparkContext, pathPrefix)
    val gl = g.withLineage(spark)

    // Create lineage directory for experiment before running it
    val lineagePath = new Path(description.lineageDir)
    val fs = lineagePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(lineagePath)

    ProvenanceContext.tracingStatus.set(description.lineageEnabled)
    ProvenanceContext.storageStatus.set(description.storageEnabled)

    ProvenanceContext.setStorageHandler(
      new HDFSStorageHandler(
        description.lineageDir,
        format = TextFile(description.compressionEnabled)
      )
    )

    val filteredGL = gl.capture(
      provenanceFilter = ProvenancePredicate(
        nodePredicate = ProvenanceGraph.allNodes,
        edgePredicate = ProvenanceGraph.allEdges
      )
    )

    val (_, elapsedTime) = TimeUtils.timed {
      description.algorithm match {
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

    val resultsPath =
      s"${description.benchmarkConfig.outputPath}/experiment-${description.experimentID}/vertices.txt"
    filteredGL.vertices.saveAsTextFile(resultsPath)

    val run = ujson.Obj(
      "duration" -> ujson.Obj(
        "amount" -> ujson.Num(elapsedTime.toDouble),
        "unit" -> "ns"
      )
    )

    if (description.lineageEnabled) {
      run("lineageDirectory") = description.lineageDir
    }

    val resultsSize = fileSize(spark, resultsPath)

    os.makeDir.all(description.outputDir)

    val dataGraphs = ProvenanceContext.graph.graph.nodes
      .filter((n: ProvenanceGraph.Type#NodeT) =>
        n.outer.g.metrics.values.toList.nonEmpty
      )
      .map((n: ProvenanceGraph.Type#NodeT) => n.outer.g)

    val sizes = dataGraphs.map((g) => {
      val size = g.storageLocation match {
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

      val loc = g.storageLocation
        .getOrElse(EmptyLocation()) match {
        case EmptyLocation()    => "<not stored>"
        case HDFSLocation(path) => path
      }

      ujson.Obj(
        "graphID" -> g.id,
        "size" -> size.toInt,
        "location" -> loc
      )
    })

    val metrics = dataGraphs.map(g => {
      ujson.Obj(
        "graphID" -> g.id,
        "metrics" -> JSONSerializer.serialize(g.metrics)
      )
    })

    val sortedSizes =
      sizes.toList.sortWith((l, r) => l("graphID").num < r("graphID").num)

    val sortedMetrics =
      metrics.toList.sortWith((l, r) => l("graphID").num < r("graphID").num)

    val parameters = ExperimentDescriptionSerializer.serialize(description)
    parameters("applicationId") = spark.sparkContext.applicationId

    val provenance = ujson.Obj(
      "inputs" -> ujson.Obj(
        "config" -> GraphUtils.configPath(pathPrefix),
        "vertices" -> GraphUtils.verticesPath(pathPrefix),
        "edges" -> GraphUtils.edgesPath(pathPrefix),
        "parameters" -> parameters
      ),
      "output" -> ujson.Obj(
        "stdout" -> (description.outputDir / "stdout.log").toString,
        "stderr" -> (description.outputDir / "stderr.log").toString,
        "results" -> resultsPath,
        "graph" -> ProvenanceContext.graph.toJson(),
        "sizes" -> ujson.Obj(
          "total" -> resultsSize.toInt,
          "individual" -> ujson.Arr(sortedSizes)
        ),
        "metrics" -> sortedMetrics
      )
    )

    os.write(
      description.outputDir / "graph.dot",
      ProvenanceContext.graph.toDot()
    )

    os.write(description.outputDir / "provenance.json", provenance)

    // Clean up lineage folder after being done with it
    fs.delete(lineagePath, true)
  }

  def fileSize(sparkSession: SparkSession, path: String): Long = {
    val p = new Path(path)
    val fs = p.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val summary = fs.getContentSummary(p)
    println(s"Checking file size at ${path}: ${summary.getLength}")
    summary.getLength
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = ParserForClass[Config].constructOrExit(args.toIndexedSeq)
    val description = parsedArgs.description
    val spark = SparkSession
      .builder()
      .appName(
        s"ProvX ${description.algorithm}/${description.dataset}/${description.lineageEnabled}/${description.runNr} benchmark"
      )
      .getOrCreate()

    val (_, elapsedTime) = run(spark, parsedArgs)
    println(s"Benchmark run took ${TimeUtils.formatNanoseconds(elapsedTime)}")
  }
}
