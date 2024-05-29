package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.{
  BenchmarkAppConfig,
  ExperimentSetup,
  GraphAlgorithm
}
import benchmark.configuration.BenchmarkAppConfig.write
import benchmark.configuration.ExperimentSetup.ExperimentSetup
import benchmark.configuration.GraphAlgorithm.GraphAlgorithm
import benchmark.utils.{GraphUtils, TimeUtils}
import provenance.{GraphLineage, ProvenanceContext, ProvenanceGraph}
import provenance.GraphLineage.graphToGraphLineage
import provenance.metrics.JSONSerializer
import provenance.query.{
  CaptureFilter,
  DataPredicate,
  GraphPredicate,
  ProvenancePredicate
}
import provenance.storage.{EmptyLocation, HDFSLocation, HDFSStorageHandler}
import provenance.ProvenanceGraph.Relation
import provenance.events.Operation

import mainargs.{arg, main, ParserForClass}
import org.apache.hadoop.fs.Path
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SparkSession

object Benchmark {
  import utils.CustomCLIArgs._

  @main
  case class Config(
      @arg(name = "config", doc = "Graphalytics benchmark configuration")
      config: BenchmarkAppConfig
  )

  def run(spark: SparkSession, args: Config): (Unit, Long) = TimeUtils.timed {

    val description = args.config
    print(description)

    val provenanceDir = description.outputDir / "provenance"
    os.makeDir.all(provenanceDir)

    val pathPrefix = s"${args.config.datasetPath}/${description.dataset}"

    val (g, config) = GraphUtils.load(spark.sparkContext, pathPrefix)
    val gl = g.withLineage(spark)

    // Create lineage directory for experiment before running it
    val lineagePath = new Path(description.lineageDir)
    val fs = lineagePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    fs.mkdirs(lineagePath)

    val (tracingEnabled, storageEnabled) =
      computeFlags(description.setup)

    val parametersDescription = write(description)
    val parameters = ujson.read(parametersDescription)
    parameters("applicationId") = spark.sparkContext.applicationId
    parameters("tracingEnabled") = tracingEnabled
    parameters("storageEnabled") = storageEnabled

    val inputs = ujson.Obj(
      "config" -> GraphUtils.configPath(pathPrefix),
      "vertices" -> GraphUtils.verticesPath(pathPrefix),
      "edges" -> GraphUtils.edgesPath(pathPrefix),
      "parameters" -> parameters
    )

    os.write(provenanceDir / "inputs.json", inputs)

    // Setup provenance configuration
    ProvenanceContext.tracingEnabled = tracingEnabled
    ProvenanceContext.storageEnabled = storageEnabled

    ProvenanceContext.setStorageHandler(
      new HDFSStorageHandler(
        description.lineageDir,
        format = description.storageFormat
      )
    )

    ProvenanceContext.setCaptureFilter(
      CaptureFilter(
        provenanceFilter = ProvenancePredicate(
          nodePredicate = ProvenanceGraph.allNodes,
          edgePredicate =
            provenanceFilter(description.setup, description.algorithm)
        ),
        dataFilter = dataFilter(description.setup, description.algorithm)
      )
    )

    // Run algorithm
    val (_, elapsedTime) = TimeUtils.timed {
      description.algorithm match {
        case GraphAlgorithm.BFS =>
          gl.bfs(config.bfs.get.sourceVertex)
        case GraphAlgorithm.PageRank =>
          gl.pageRank(numIter = config.pr.get.numIterations)
        case GraphAlgorithm.SSSP =>
          gl.sssp(config.sssp.get.sourceVertex)
        case GraphAlgorithm.WCC => gl.wcc()
      }
    }
    println(s"Run took ${TimeUtils.formatNanoseconds(elapsedTime)}")

    val resultsPath =
      s"${description.outputDir}/experiment-${description.experimentID}/vertices.txt"
    gl.vertices.saveAsTextFile(resultsPath)

    val dataGraphs = ProvenanceContext.graph.graph.nodes
      .filter((n: ProvenanceGraph.Type#NodeT) =>
        n.outer.g.metrics.values.toList.nonEmpty || n.outer.g.storageLocation.isDefined
      )
      .map((n: ProvenanceGraph.Type#NodeT) => n.outer.g)

    val sizes = dataGraphs.map(g => {
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

    val outputs = ujson.Obj(
      "stdout" -> (description.outputDir / "stdout.log").toString,
      "stderr" -> (description.outputDir / "stderr.log").toString,
      "results" -> resultsPath,
      "graph" -> ProvenanceContext.graph.toJson(),
      "sizes" -> sortedSizes,
      "duration" -> ujson.Obj(
        "amount" -> ujson.Num(elapsedTime.toDouble),
        "unit" -> "ns"
      ),
      "metrics" -> sortedMetrics
    )

    if (description.setup != ExperimentSetup.Baseline) {
      outputs("lineageDirectory") = description.lineageDir
    }

    os.write(provenanceDir / "outputs.json", outputs)

    os.write(
      description.outputDir / "graph.dot",
      ProvenanceContext.graph.toDot()
    )

    // Clean up lineage folder after being done with it
    fs.delete(lineagePath, true)
  }

  def computeFlags(expSetup: ExperimentSetup): (Boolean, Boolean) = {
    val tracingEnabled = expSetup match {
      case ExperimentSetup.Baseline => false
      case _                        => true
    }

    val storageEnabled = expSetup match {
      case ExperimentSetup.Baseline => false
      case ExperimentSetup.Tracing  => false
      case _                        => true
    }

    (tracingEnabled, storageEnabled)
  }

  def dataFilter(
      experimentSetup: ExperimentSetup,
      algorithm: GraphAlgorithm
  ): DataPredicate = {
    experimentSetup match {
      case ExperimentSetup.DataGraphPruning | ExperimentSetup.CombinedPruning =>
        algorithm match {
          case GraphAlgorithm.BFS | GraphAlgorithm.WCC =>
            GraphPredicate(
              nodePredicate = (_: VertexId, value: Any) => {
                value.asInstanceOf[Long] != Long.MaxValue
              }
            )
          case GraphAlgorithm.PageRank =>
            GraphPredicate(
              nodePredicate = (_: VertexId, value: Any) => {
                value.toString.toDouble != 0.0
              }
            )
//            val ids =
//              gl.vertices
//                .sample(withReplacement = false, 0.1)
//                .mapValues(v => 0.0.asInstanceOf[Any])
//            DeltaPredicate(ids)
          case GraphAlgorithm.SSSP =>
            GraphPredicate(
              nodePredicate = (_: VertexId, value: Any) => {
                value.asInstanceOf[Double] != Double.PositiveInfinity
              }
            )
        }
      case _ =>
        GraphPredicate(
          nodePredicate = (_: VertexId, _: Any) => true
        )
    }
  }

  def provenanceFilter(
      expSetup: ExperimentSetup,
      algorithm: GraphAlgorithm
  ): Relation => Boolean = {
    expSetup match {
      case ExperimentSetup.ProvenanceGraphPruning |
          ExperimentSetup.CombinedPruning =>
        (r: ProvenanceGraph.Relation) => {
          r.edge.event match {
            case Operation("outerJoinVertices") =>
              algorithm == GraphAlgorithm.fromString("pr")
            case Operation("joinVertices") =>
              algorithm != GraphAlgorithm.fromString("pr")
            case _ => false
          }
        }
      case _ =>
        (_: ProvenanceGraph.Relation) => {
          true
        }
    }
  }

  private def fileSize(sparkSession: SparkSession, path: String): Long = {
    val p = new Path(path)
    val fs = p.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val summary = fs.getContentSummary(p)
    // Only call getLength once otherwise it returns two different sizes and only the first one is correct!
    val size = summary.getLength
    println(s"Checking file size at $path: ${size}")
    size
  }

  def main(args: Array[String]): Unit = {
    val parsedArgs = ParserForClass[Config].constructOrExit(args.toIndexedSeq)
    val config = parsedArgs.config
    val spark = SparkSession
      .builder()
      .appName(
        s"ProvX ${config.algorithm}/${config.dataset}/${config.setup}/${config.runNr} benchmark"
      )
      .getOrCreate()

    val (_, elapsedTime) = run(spark, parsedArgs)
    println(s"Benchmark run took ${TimeUtils.formatNanoseconds(elapsedTime)}")
  }
}
