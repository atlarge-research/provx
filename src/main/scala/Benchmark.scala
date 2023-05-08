package lu.magalhaes.gilles.provxlib

import lineage.GraphLineage._
import lineage.{LineageContext, PregelIterationMetrics}
import utils.GraphalyticsConfiguration

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object Benchmark {
  val datasets = Seq("kgs", "wiki-Talk")

  def loadGraph(sc: SparkContext, datasetPathPrefix: String, name: String): (Graph[Unit, Unit], GraphalyticsConfiguration) = {
    // TODO(gm): copy files to HDFS
    val edgePath = s"file://${datasetPathPrefix}/${name}.e"
    val vertexPath = s"file://${datasetPathPrefix}/${name}.v"
    val edges = sc.textFile(edgePath).map(line => {
      val tokens = line.trim.split("""\s""")
      Edge(tokens(0).toLong, tokens(1).toLong, ())
    })

    val vertices = sc.textFile(vertexPath).map(line => {
      val tokens = line.trim.split("""\s""")
      (tokens(0).toLong, ())
    })

    val config = new GraphalyticsConfiguration(s"${datasetPathPrefix}/${name}.properties")

    (Graph(vertices, edges), config)
  }

  def main(args: Array[String]) {
    require(args.length >= 3, "Args required: <graph dataset path prefix> <metrics results> <lineage directory>")
    val datasetPathPrefix = args(0) // /var/scratch/gmo520/thesis/benchmark/graphs/xs
    val metricsPathPrefix = args(1) // /var/scratch/gmo520/thesis/results
    val lineagePathPrefix = args(2) // /local/gmo520

    val spark = SparkSession.builder.appName("ProvX benchmark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    LineageContext.setLineageDir(spark.sparkContext, lineagePathPrefix)

    for (dataset <- datasets) {
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