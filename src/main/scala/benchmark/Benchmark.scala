package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.{
  BenchmarkAppConfig,
  ExperimentSetup,
  GraphAlgorithm
}
import benchmark.configuration.ExperimentSetup.ExperimentSetup
import benchmark.utils.{GraphUtils, TimeUtils}
import provenance.{ProvenanceContext, ProvenanceGraph}
import provenance.GraphLineage.graphToGraphLineage
import provenance.metrics.JSONSerializer
import provenance.query.{DataPredicate, ProvenancePredicate}
import provenance.storage.{
  EmptyLocation,
  HDFSLocation,
  HDFSStorageHandler,
  TextFile
}
import provenance.ProvenanceGraph.Relation
import provenance.events.{
  Operation,
  BFS => ETBFS,
  PageRank => ETPageRank,
  SSSP => ETSSSP,
  WCC => ETWCC
}

import lu.magalhaes.gilles.provxlib.benchmark.configuration.BenchmarkAppConfig.write
import lu.magalhaes.gilles.provxlib.benchmark.configuration.GraphAlgorithm.GraphAlgorithm
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
    parameters("executorCount") =
      spark.sparkContext.getExecutorMemoryStatus.keys.toList.length

    val inputs = ujson.Obj(
      "config" -> GraphUtils.configPath(pathPrefix),
      "vertices" -> GraphUtils.verticesPath(pathPrefix),
      "edges" -> GraphUtils.edgesPath(pathPrefix),
      "parameters" -> parameters
    )

    os.write(provenanceDir / "inputs.json", inputs)

    // Setup provenance configuration
    ProvenanceContext.tracingStatus.set(tracingEnabled)
    ProvenanceContext.storageStatus.set(storageEnabled)

    ProvenanceContext.setStorageHandler(
      new HDFSStorageHandler(
        description.lineageDir,
        format = description.storageFormat
      )
    )

    val filteredGL = gl.capture(
      provenanceFilter = ProvenancePredicate(
        nodePredicate = ProvenanceGraph.allNodes,
        edgePredicate = provenanceFilter(description.setup)
      ),
      dataFilter = DataPredicate(
        nodePredicate = dataFilter(description.setup, description.algorithm)
      )
    )

    // Run algorithm
    val (_, elapsedTime) = TimeUtils.timed {
      description.algorithm match {
        case GraphAlgorithm.BFS =>
          filteredGL.bfs(config.bfs.get.sourceVertex)
        case GraphAlgorithm.PageRank =>
          filteredGL.pageRank(numIter = config.pr.get.numIterations)
        case GraphAlgorithm.SSSP =>
          filteredGL.sssp(config.sssp.get.sourceVertex)
        case GraphAlgorithm.WCC => filteredGL.wcc()
      }
    }
    println(s"Run took ${TimeUtils.formatNanoseconds(elapsedTime)}")

    val resultsPath =
      s"${description.outputDir}/experiment-${description.experimentID}/vertices.txt"
    filteredGL.vertices.saveAsTextFile(resultsPath)

    val resultsSize = fileSize(spark, resultsPath)

    val dataGraphs = ProvenanceContext.graph.graph.nodes
      .filter((n: ProvenanceGraph.Type#NodeT) =>
        n.outer.g.metrics.values.toList.nonEmpty
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
      "sizes" -> ujson.Obj(
        "total" -> resultsSize.toInt,
        "individual" -> sortedSizes
      ),
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
  ): (VertexId, Any) => Boolean = {
    experimentSetup match {
      case ExperimentSetup.SmartPruning | ExperimentSetup.Combined =>
        algorithm match {
          case GraphAlgorithm.BFS | GraphAlgorithm.WCC =>
            (_: VertexId, value: Any) => {
              value.asInstanceOf[Long] != Long.MaxValue
            }
          case GraphAlgorithm.PageRank =>
            (_: VertexId, _: Any) => {
              // TODO: figure out how to filter PageRank
              // TODO: get previous generation and compare if change is more than a certain delta
              true
            }
          case GraphAlgorithm.SSSP =>
            (_: VertexId, value: Any) => {
              value.asInstanceOf[Double] != Double.PositiveInfinity
            }
        }
      case _ =>
        (_: VertexId, _: Any) => true
    }
  }

  def provenanceFilter(expSetup: ExperimentSetup): Relation => Boolean = {
    expSetup match {
      case ExperimentSetup.AlgorithmOpOnly | ExperimentSetup.Combined =>
        (r: ProvenanceGraph.Relation) => {
          r.edge.event match {
            case ETPageRank(_, _) | ETBFS(_) | ETSSSP(_) | ETWCC(_) => true
            case _                                                  => false
          }
        }
      case ExperimentSetup.JoinVerticesOpOnly =>
        (r: ProvenanceGraph.Relation) => {
          r.edge.event match {
            case Operation("joinVertices") => true
            case _                         => false
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
    println(s"Checking file size at ${path}: ${summary.getLength}")
    summary.getLength
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
