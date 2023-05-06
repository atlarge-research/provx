package lu.magalhaes.gilles.provxlib

import lineage.GraphLineage._
import lineage.{LineageContext, PregelIterationMetrics}
import utils.GraphalyticsConfiguration

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object Benchmark {
  val datasets = Seq("kgs", "wiki-Talk")
  val datasetPathPrefix = "/var/scratch/gmo520/thesis/benchmark/graphs/xs"
  val metricsDestinationPrefix = "/var/scratch/gmo520/thesis/results"

  def loadGraph(sc: SparkContext, name: String): Graph[Unit, Unit] = {
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

    Graph(vertices, edges)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("ProvX benchmark").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    LineageContext.setLineageDir(spark.sparkContext, "file:///tmp/provx-lineage")
    LineageContext.disableCheckpointing()

    for (dataset <- datasets) {
      val g = loadGraph(spark.sparkContext, dataset)
      val gl = g.withLineage()
      val config = new GraphalyticsConfiguration(s"${datasetPathPrefix}/${dataset}.properties")

      for (algorithm <- config.algorithms().get) {
        println("---")
        println(s"algorithm: ${algorithm}, graph: ${dataset}")

        val results = ujson.Obj(
          "algorithm" -> algorithm,
          "graph" -> dataset
        )

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
        println(f"Took ${elapsedTime}%.2fs")
        results("duration") = ujson.Obj(
          "duration" -> ujson.Num(elapsedTime),
          "unit" -> "ns"
        )
        val iterationMetadata = ujson.Arr()

        if (sol.isDefined && sol.get.getMetrics().isDefined) {
          val metrics = sol.get.getMetrics().get
          for ((iterationMetrics, idx) <- metrics.getIterations().zipWithIndex) {
            iterationMetadata.arr.append(ujson.Obj(
              "idx" -> idx,
              "messageCount" -> ujson.Num(iterationMetrics.getMessageCount())
            ))
          }
          results("iterations") = iterationMetadata
        }
        os.write(os.Path(s"${metricsDestinationPrefix}/${algorithm}-${dataset}.json"), results)
      }
    }
  }
}