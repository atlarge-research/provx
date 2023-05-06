package lu.magalhaes.gilles.provxlib
package lineage.lib

import lineage.{LineagePregel, PregelMetrics}

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object LineagePageRank extends Logging {

  def run[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED],
    tol: Double,
    numIter: Int = Int.MaxValue,
    resetProb: Double = 0.15,
    sampleFraction: Option[Double] = None): (Graph[Double, Double], PregelMetrics) =
  {
    require(resetProb >= 0 && resetProb <= 1, s"Random reset probability must belong" +
      s" to [0, 1], but got ${resetProb}")
    require(tol >= 0, s"Tolerance must be no less than 0, but got ${tol}")

    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr)
      // Set the vertex attributes to (initialPR, delta = 0)
      .mapVertices { (id, attr) => (0.0, 0.0) }
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]): Iterator[(VertexId, Double)] = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = resetProb / (1.0 - resetProb)

    def pruneLineage(v: (VertexId, (Double, Double))): Boolean = {
      !(v._2._2 < tol)
    }

    val (rankGraph, metrics) = LineagePregel(
      pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out,
      pruneLineage = Some(pruneLineage(_)),
      maxIterations = numIter,
      sampleFraction = sampleFraction
    )(
      vertexProgram, sendMessage, messageCombiner)

    val mappedRankGraph = rankGraph
      .mapVertices((_, attr) => attr._1)

    // SPARK-18847 If the graph has sinks (vertices with no outgoing edges) correct the sum of ranks
    (normalizeRankSum(mappedRankGraph), metrics)
  }

  // Normalizes the sum of ranks to n
  private def normalizeRankSum(rankGraph: Graph[Double, Double]): Graph[Double, Double] = {
    val rankSum = rankGraph.vertices.values.sum()
    val numVertices = rankGraph.numVertices
    val correctionFactor = numVertices.toDouble / rankSum
    rankGraph.mapVertices((id, rank) => rank * correctionFactor)
  }

  // TODO(gm): GraphX also has version where you can run with previous PageRank graph
  // but was removed in ProvX to simplify code
}
