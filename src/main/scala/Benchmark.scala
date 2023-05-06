package lu.magalhaes.gilles.provxlib

import lu.magalhaes.gilles.provxlib.lineage.{GraphLineage, LineageContext}
import lu.magalhaes.gilles.provxlib.lineage.GraphLineage._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object Benchmark {
  val datasets = Seq("kgs", "wiki-Talk")
  // TODO(gm): copy files to HDFS
  val datasetPathPrefix = "file:///var/scratch/gmo520/thesis/benchmark/graphs/xs"
  val metricsDestinationPrefix = "/var/scratch/gmo520/thesis/results"

  val datasetParameters = Map[String, Map[String, Int]](
    "kgs" -> Map[String, Int](
      "bfs" -> 1,
      "pagerank" -> 20,
      "sssp" -> 1
    ),
    "wiki-Talk" -> Map[String, Int](
      "bfs" -> 1,
      "pagerank" -> 20,
      "sssp" -> 1
    )
  )

  def loadGraph(sc: SparkContext, name: String): Graph[Unit, Unit] = {
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

    Graph(vertices, edges)
  }


  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("ProvX benchmark").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    LineageContext.setLineageDir(spark.sparkContext, "file:///tmp/provx-lineage")
    LineageContext.disableCheckpointing()

    val algorithms = Map[String, (GraphLineage[_, _], Int) => GraphLineage[_, _]](
      "bfs" -> ((g, v) => g.bfs(v)),
      "pagerank" -> ((g, v) => g.pageRank(numIter = v)),
      // "cdlp" -> ((g, _) => g.cdlp()),
      // "lcc" -> ((g, _) => g.lcc()),
      "wcc" -> ((g, _) => g.connectedComponents()),
      "sssp" -> ((g, v) => g.sssp(v)),
    )

    for (dataset <- datasets) {
      val g = loadGraph(spark.sparkContext, dataset)
        .withLineage()

      val params = datasetParameters(dataset)

      for ((algorithm, f) <- algorithms) {
        println("---")
        println(s"algorithm: ${algorithm}, graph: ${dataset}")

        val param = params.getOrElse(algorithm, 0)

        val startTime = System.nanoTime()
        val sol = f(g, param)
        val endTime = System.nanoTime()

        val elapsedTime = (endTime - startTime) / 1e9 // in ms
        println(f"Took ${elapsedTime}%.2fs")

        val metrics = sol.getMetrics()
        if (metrics.isDefined) {
          metrics.get
            .saveAsTextFile(s"${metricsDestinationPrefix}/${algorithm}-${dataset}.txt")
        }
      }
    }
  }
}