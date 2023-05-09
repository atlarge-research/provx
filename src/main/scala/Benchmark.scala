package lu.magalhaes.gilles.provxlib

import lineage.GraphLineage._
import lineage.{LineageContext, PregelIterationMetrics}
import utils.{BenchmarkConfig, GraphalyticsConfiguration}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object Benchmark {
  val datasets = Seq("kgs", "wiki-Talk")

  def loadGraph(sc: SparkContext, datasetPathPrefix: String, name: String): (Graph[Unit, Unit], GraphalyticsConfiguration) = {
    // TODO(gm): copy files to HDFS
    val edgePath = s"${datasetPathPrefix}/${name}.e"
    val vertexPath = s"${datasetPathPrefix}/${name}.v"
    val edges = sc.textFile(edgePath).map(line => {
      val tokens = line.trim.split("""\s""")
      Edge(tokens(0).toLong, tokens(1).toLong, ())
    })

    val vertices = sc.textFile(vertexPath).map(line => {
      val tokens = line.trim.split("""\s""")
      (tokens(0).toLong, ())
    })

    val config = new GraphalyticsConfiguration(sc.hadoopConfiguration, s"${datasetPathPrefix}/${name}.properties")

    (Graph(vertices, edges), config)
  }

  def main(args: Array[String]) {
    require(args.length >= 1, "Args required: <config>")

    val benchmarkConfig = new BenchmarkConfig(args(0))

    val datasetPathPrefix = benchmarkConfig.datasetPath.get
    val metricsPathPrefix = benchmarkConfig.metricsPath.get
    val lineagePathPrefix = benchmarkConfig.lineagePath.get

    println(s"Dataset path: ${datasetPathPrefix}")
    println(s"Metrics path: ${metricsPathPrefix}")
    println(s"Lineage path: ${lineagePathPrefix}")

    val spark = SparkSession.builder.appName("ProvX benchmark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    LineageContext.setLineageDir(spark.sparkContext, lineagePathPrefix)

    benchmarkConfig.graphs.get.foreach(println)

    for (dataset <- benchmarkConfig.graphs.get) {
      val (g, config) = loadGraph(spark.sparkContext, datasetPathPrefix, dataset)
      val gl = g.withLineage()

      for (algorithm <- config.algorithms().get) {
        println("---")
        println(s"algorithm: ${algorithm}, graph: ${dataset}")

        val results = ujson.Obj(
          "algorithm" -> algorithm,
          "graph" -> dataset
        )

        def runAlgorithm(saveMetadata: Boolean = false): ujson.Obj = {
          val startTime = System.nanoTime()
          val sol = algorithm match {
            case "bfs" => Some(gl.bfs(config.bfsSourceVertex()))
            case "wcc" => Some(gl.connectedComponents())
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

          if (saveMetadata && sol.isDefined && sol.get.getMetrics().isDefined) {
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

          run
        }

        LineageContext.disableCheckpointing()
        results("no-lineage") = runAlgorithm()
        LineageContext.enableCheckpointing()
        results("with-lineage") = runAlgorithm(saveMetadata = true)


        os.write(os.Path(s"${metricsPathPrefix}/${algorithm}-${dataset}.json"), results)
      }
    }
  }
}