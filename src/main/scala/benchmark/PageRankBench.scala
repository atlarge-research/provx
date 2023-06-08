package lu.magalhaes.gilles.provxlib
package benchmark

import lineage.GraphLineage.graphToGraphLineage
import lineage.LineageContext

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

object PageRankBench {
  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Not enough arguments provided.")
    val datasetPrefix = args(0)
    val lineageStorePrefix = args(1)

    println(s"Dataset prefix: ${datasetPrefix}")
    println(s"Lineage storage prefix: ${lineageStorePrefix}")

    val spark = SparkSession.builder.appName("ProvX benchmark").getOrCreate()

    LineageContext.setLineageDir(spark.sparkContext, lineageStorePrefix)

    val edges = spark.sparkContext
      .textFile(s"${datasetPrefix}.e")
      .map(line => {
        val tokens = line.trim.split("""\s""")
        Edge(tokens(0).toLong, tokens(1).toLong, null)
      })

//      .textFile("file:///Users/gm/vu/thesis/benchmark/data/graphs-xs/wiki-Talk.v")
    val vertices = spark.sparkContext
      .textFile(s"${datasetPrefix}.v")
      .map(line => {
        val tokens = line.trim.split("""\s""")
        (tokens(0).toLong, null)
      })

    val graph = Graph(vertices, edges)


    val gl = graph.withLineage()

    gl.pageRank(numIter = 15)
  }
}
