package lu.magalhaes.gilles.provxlib
package utils

import benchmark.utils.GraphUtils
import provenance.GraphLineage

import org.apache.spark.SparkContext

object GraphTestLoader {
  def load(
      sc: SparkContext,
      algorithm: String
  ): (GraphLineage[Unit, Double], String) = {
    val expectedOutput =
      getClass.getResource(s"/example-directed-${algorithm}").toString
    val parent =
      "/" + expectedOutput.split("/").drop(1).dropRight(1).mkString("/")
    val (graph, _) = GraphUtils.load(sc, parent + "/example-directed")
    (GraphLineage(graph), expectedOutput)
  }
}
