package lu.magalhaes.gilles.provxlib
package benchmark.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}

object GraphUtils {
  def load(sc: SparkContext, datasetPathPrefix: String, name: String): (Graph[Unit, Double], GraphalyticsConfiguration) = {
    val config = new GraphalyticsConfiguration(sc.hadoopConfiguration, s"${datasetPathPrefix}/${name}.properties")

    val edgePath = s"${datasetPathPrefix}/${name}.e"
    val vertexPath = s"${datasetPathPrefix}/${name}.v"

    val edges = sc.textFile(edgePath).map(line => {
      val tokens = line.trim.split("""\s""")
      if (tokens.length == 3) {
        Edge(tokens(0).toLong, tokens(1).toLong, tokens(2).toDouble)
      } else {
        Edge(tokens(0).toLong, tokens(1).toLong, 0.0)
      }
    })

    val vertices = sc.textFile(vertexPath).map(line => {
      val tokens = line.trim.split("""\s""")
      (tokens(0).toLong, ())
    })

    (Graph(vertices, edges), config)
  }
}
