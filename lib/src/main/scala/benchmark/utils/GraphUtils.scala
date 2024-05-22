package lu.magalhaes.gilles.provxlib
package benchmark.utils

import benchmark.configuration.{GraphalyticsConfig, GraphConfig}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}

object GraphUtils {
  def verticesPath(prefix: String) = s"$prefix.v"
  def edgesPath(prefix: String) = s"$prefix.e"
  def configPath(prefix: String) = s"$prefix.properties"

  def load(
      sc: SparkContext,
      prefix: String
  ): (Graph[Unit, Double], GraphConfig) = {
    val config = GraphalyticsConfig.loadHadoop(configPath(prefix))

    val edgePath = edgesPath(prefix)
    val vertexPath = verticesPath(prefix)

    val edges = sc
      .textFile(edgePath)
      .map(line => {
        val tokens = line.trim.split("""\s""")
        if (tokens.length == 3) {
          Edge(tokens(0).toLong, tokens(1).toLong, tokens(2).toDouble)
        } else {
          Edge(tokens(0).toLong, tokens(1).toLong, 0.0)
        }
      })

    val vertices = sc
      .textFile(vertexPath)
      .map(line => {
        val tokens = line.trim.split("""\s""")
        (tokens(0).toLong, ())
      })

    (Graph(vertices, edges), config)
  }
}
