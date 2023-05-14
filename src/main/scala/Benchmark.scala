package lu.magalhaes.gilles.provxlib

import lineage.GraphLineage._
import lineage.{LineageContext, PregelIterationMetrics}
import utils.{BenchmarkConfig, GraphalyticsConfiguration}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object Benchmark {

  def loadGraph(sc: SparkContext, datasetPathPrefix: String, name: String): (Graph[Unit, Double], GraphalyticsConfiguration) = {
    val edgePath = s"${datasetPathPrefix}/${name}.e"
    val vertexPath = s"${datasetPathPrefix}/${name}.v"
    val edges = sc.textFile(edgePath).map(line => {
      val tokens = line.trim.split("""\s""")
      Edge(tokens(0).toLong, tokens(1).toLong, tokens(2).toDouble)
    })

    val vertices = sc.textFile(vertexPath).map(line => {
      val tokens = line.trim.split("""\s""")
      (tokens(0).toLong, ())
    })

    val config = new GraphalyticsConfiguration(sc.hadoopConfiguration, s"${datasetPathPrefix}/${name}.properties")

    (Graph(vertices, edges), config)
  }

  def main(args: Array[String]) {
    require(args.length >= 4, "Args required: <config> <algorithm> <dataset> <lineage|no-lineage>")

    val totalStartTime = System.nanoTime()

    val configPath = args(0)
    val algorithm = args(1)
    val dataset = args(2)
    val lineageOption = args(3) == "lineage"

    val benchmarkConfig = new BenchmarkConfig(configPath)

    val datasetPathPrefix = benchmarkConfig.datasetPath.get
    val metricsPathPrefix = benchmarkConfig.metricsPath.get
    val lineagePathPrefix = benchmarkConfig.lineagePath.get
    val outputPath = benchmarkConfig.outputPath.get

    println(s"Dataset path: ${datasetPathPrefix}")
    println(s"Metrics path: ${metricsPathPrefix}")
    println(s"Lineage path: ${lineagePathPrefix}")
    println(s"Output  path: ${outputPath}")

    val spark = SparkSession.builder
      .appName(s"ProvX ${algorithm}/${dataset}/${lineageOption} benchmark")
      .getOrCreate()

    LineageContext.setLineageDir(spark.sparkContext, lineagePathPrefix)
    if (lineageOption) {
      LineageContext.enableCheckpointing()
    } else {
      LineageContext.disableCheckpointing()
    }

    val (g, config) = loadGraph(spark.sparkContext, datasetPathPrefix, dataset)
    val gl = g.withLineage()

    println("---")
    println(s"algorithm: ${algorithm}, graph: ${dataset}")

    val startTime = System.nanoTime()
    val sol = algorithm match {
      case "bfs" => Some(gl.bfs(config.bfsSourceVertex()))
      case "wcc" => Some(gl.wcc())
      case "pr" => Some(gl.pageRank(numIter = config.pageRankIterations()))
      case "sssp" => Some(gl.sssp(config.ssspSourceVertex()))
      case "lcc" => None // broken: Some(gl.lcc())
      case "cdlp" => None // broken: Some(gl.cdlp())
    }
    val endTime = System.nanoTime()
    val elapsedTime = endTime - startTime
    println(f"Took ${elapsedTime / 10e9}%.2fs")

    val run = ujson.Obj()
    val iterationMetadata = ujson.Arr()

    if (lineageOption && sol.isDefined && sol.get.getMetrics().isDefined) {
      val metrics = sol.get.getMetrics().get
      for ((iterationMetrics, idx) <- metrics.getIterations().zipWithIndex) {
        iterationMetadata.arr.append(ujson.Obj(
          "idx" -> idx,
          "messageCount" -> ujson.Num(iterationMetrics.getMessageCount())
        ))
      }
      run("iterations") = iterationMetadata
      run("lineageDirectory") = metrics.getLineageDirectory()
    }

    run("duration") = ujson.Obj(
      "duration" -> ujson.Num(elapsedTime),
      "unit" -> "ns"
    )

    val postfix = if (lineageOption) { "-lineage" } else { "" }
    g.vertices.saveAsTextFile(s"${outputPath}/${algorithm}-${dataset}${postfix}.txt")

    val results = ujson.Obj(
      "algorithm" -> algorithm,
      "graph" -> dataset,
      "lineage" -> lineageOption,
      "metadata" -> run
    )

    os.write(os.Path(s"${metricsPathPrefix}/${algorithm}-${dataset}${postfix}.json"), results)

    val totalEndTime = System.nanoTime()
    println(f"Benchmark run took ${(totalEndTime - totalStartTime) / 10e9}%.2fs")
  }
}