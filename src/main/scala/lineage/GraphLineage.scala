package lu.magalhaes.gilles.provxlib
package lineage

import lineage.lib._

import org.apache.spark.graphx.{Graph, VertexId}

import scala.language.implicitConversions
import scala.reflect.ClassTag

class GraphLineage[VD: ClassTag, ED: ClassTag]
    (graph: Graph[VD, ED], metrics: Option[PregelMetrics] = None) {

  def getGraph(): Graph[VD, ED] = graph

  private var sampleFraction: Option[Double] = None

  def getMetrics(): Option[PregelMetrics] = metrics

  def withLineage(): GraphLineage[VD, ED] = this

  // TODO: add actual provenance graph that can be exported

  def pageRank(tol: Double = 0.01, resetProb: Double = 0.15,
       numIter: Int = Int.MaxValue): GraphLineage[Double, Double] = {
    // scalastyle:off println
    println("Using lineage-enabled PageRank.")
    // scalastyle:on println

    val (g, pregelMetrics) = LineagePageRank.run(
      graph, tol, numIter = numIter, resetProb = resetProb
    )
    new GraphLineage[Double, Double](g, Some(pregelMetrics))
  }

  def bfs(sourceVertex: VertexId): GraphLineage[Long, ED] = {
    val (g, pregelMetrics) = LineageBFS.run(graph, sourceVertex)
    new GraphLineage[Long, ED](g, Some(pregelMetrics))
  }

  def wcc(maxIterations: Int = Int.MaxValue): GraphLineage[VertexId, ED] = {
    val (g, pregelMetrics) = LineageWCC.run(graph, maxIterations = maxIterations)
    new GraphLineage[VertexId, ED](g, Some(pregelMetrics))
  }

  def sssp(source: VertexId): GraphLineage[Double, Double] = {
    val (g, pregelMetrics) = LineageSSSP.run(graph, source)
    new GraphLineage[Double, Double](g, Some(pregelMetrics))
  }

  def cdlp(): GraphLineage[VertexId, Unit] = {
    val (g, pregelMetrics) = LineageCDLP.run(graph)
    new GraphLineage[VertexId, Unit](g, Some(pregelMetrics))
  }

  def lcc(): Graph[Double, Unit] = {
    LineageLCC.run(graph)
  }

  def sample(fraction: Double): GraphLineage[VD, ED] = {
    assert(fraction > 0 && fraction <= 1.0, "Fraction must be between 0 and 1")
    sampleFraction = Some(fraction)
    this
  }
}

object GraphLineage {

  implicit def graphToGraphLineage[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphLineage[VD, ED] = {
    new GraphLineage[VD, ED](g)
  }
}
