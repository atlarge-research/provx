package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.ExperimentParameters.{BFS, PageRank, SSSP, WCC}
import benchmark.utils.{GraphUtils, TimeUtils}
import provenance.{GraphLineage, ProvenanceContext, ProvenanceGraph}
import provenance.GraphLineage.graphToGraphLineage
import provenance.metrics.JSONSerializer
import provenance.query.{DataPredicate, ProvenancePredicate}
import provenance.storage.{
  EmptyLocation,
  HDFSLocation,
  HDFSStorageHandler,
  TextFile
}

import lu.magalhaes.gilles.provxlib.provenance.ProvenanceGraph.Relation
import mainargs.{arg, main, ParserForClass}
import org.apache.hadoop.fs.Path
import org.apache.spark.graphx.{EdgeTriplet, VertexId}
import org.apache.spark.sql.SparkSession
import provenance.events.{
  Operation,
  BFS => ETBFS,
  PageRank => ETPageRank,
  SSSP => ETSSSP,
  WCC => ETWCC
}

import lu.magalhaes.gilles.provxlib.benchmark.ExperimentSetup.ExperimentSetup
import lu.magalhaes.gilles.provxlib.benchmark.configuration.GraphalyticsConfiguration

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

    val expSetup = ExperimentSetup.values.find(_.toString == description.setup)
    if (expSetup.isEmpty) {
      println("Unknown experiment setup! Quitting...")
      return ((), 0)
    }

    val (tracingEnabled, storageEnabled, compressionEnabled) =
      computeFlags(expSetup.get)

    ProvenanceContext.tracingStatus.set(tracingEnabled)
    ProvenanceContext.storageStatus.set(storageEnabled)

    ProvenanceContext.setStorageHandler(
      new HDFSStorageHandler(
        description.lineageDir,
        format = TextFile(compressionEnabled)
      )
    )

    val filteredGL = gl.capture(
      provenanceFilter = ProvenancePredicate(
        nodePredicate = ProvenanceGraph.allNodes,
        edgePredicate = provenanceFilter(expSetup.get)
      ),
      dataFilter = DataPredicate(
        nodePredicate = dataFilter(expSetup.get, description.algorithm)
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
    parameters("tracingEnabled") = tracingEnabled
    parameters("storageEnabled") = storageEnabled

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

    if (description.setup != ExperimentSetup.Baseline.toString) {
      outputs("lineageDirectory") = description.lineageDir
    }

    val provenance = ujson.Obj(
      "inputs" -> ujson.Obj(
        "config" -> GraphUtils.configPath(pathPrefix),
        "vertices" -> GraphUtils.verticesPath(pathPrefix),
        "edges" -> GraphUtils.edgesPath(pathPrefix),
        "parameters" -> parameters
      ),
      "outputs" -> outputs
    )

    os.write(
      description.outputDir / "graph.dot",
      ProvenanceContext.graph.toDot()
    )

    os.write(description.outputDir / "provenance.json", provenance)

    // Clean up lineage folder after being done with it
    fs.delete(lineagePath, true)
  }

  def computeFlags(expSetup: ExperimentSetup): (Boolean, Boolean, Boolean) = {
    val tracingEnabled = expSetup match {
      case ExperimentSetup.Baseline => false
      case _                        => true
    }

    val storageEnabled = expSetup match {
      case ExperimentSetup.Baseline => false
      case ExperimentSetup.Tracing  => false
      case _                        => true
    }

    val compressionFlag = expSetup match {
      case ExperimentSetup.Compression => true
      case ExperimentSetup.Combined    => true
      case _                           => false
    }

    (tracingEnabled, storageEnabled, compressionFlag)
  }

  def dataFilter(
      experimentSetup: ExperimentSetup,
      algorithm: ExperimentParameters.Algorithm
  ): (VertexId, Any) => Boolean = {
    experimentSetup match {
      case ExperimentSetup.SmartPruning | ExperimentSetup.Combined =>
        algorithm match {
          case WCC() | BFS() =>
            (_: VertexId, value: Any) => {
              value.asInstanceOf[Long] != Long.MaxValue
            }
          case PageRank() =>
            (_: VertexId, _: Any) => {
              // TODO: figure out how to filter PageRank
              // TODO: get previous generation and compare if change is more than a certain delta
              true
            }
          case SSSP() =>
            (_: VertexId, value: Any) => {
              value.asInstanceOf[Double] != Double.PositiveInfinity
            }
          case _ =>
            throw new NotImplementedError("unknown graph algorithm")
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
        s"ProvX ${description.algorithm}/${description.dataset}/${description.setup}/${description.runNr} benchmark"
      )
      .getOrCreate()

    val (_, elapsedTime) = run(spark, parsedArgs)
    println(s"Benchmark run took ${TimeUtils.formatNanoseconds(elapsedTime)}")
  }
}
