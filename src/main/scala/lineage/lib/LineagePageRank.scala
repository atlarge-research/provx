package lu.magalhaes.gilles.provxlib
package lineage.lib

import lineage.{LineagePregel, PregelMetrics}

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object LineagePageRank extends Logging {

  def run[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED],
    numIter: Int,
    dampingFactor: Double = 0.85,
    sampleFraction: Option[Double] = None): (Graph[Double, Unit], PregelMetrics) =
  {
    val vertexCount = graph.numVertices

    val pagerankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (_, _, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr)
      .mapVertices { (_, _) => 1.0 / vertexCount }
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, oldPR: Double, msgSum: Double): Double = {
      (1.0 - dampingFactor) / vertexCount + dampingFactor * (msgSum + oldPR / vertexCount)
    }

    def sendMessage(edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] =
      Iterator((edge.dstId, edge.srcAttr * edge.attr))

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = 1.0 / vertexCount

    val (rankGraph, metrics) = LineagePregel(
      pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out,
      maxIterations = numIter,
      sampleFraction = sampleFraction
    )(
      vertexProgram, sendMessage, messageCombiner)

    (rankGraph.mapEdges(_ => Unit), metrics)
  }
}
