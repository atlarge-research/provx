package lu.magalhaes.gilles.provxlib
package lineage

import lineage.algorithms._
import lineage.metrics.ObservationSet

import org.apache.spark.graphx.{Graph, VertexId}

import scala.language.implicitConversions
import scala.reflect.ClassTag

class GraphLineage[VD: ClassTag, ED: ClassTag]
    (graph: Graph[VD, ED], metrics: ObservationSet) {

  def getGraph(): Graph[VD, ED] = graph

  private var sampleFraction: Option[Double] = None

  def getMetrics(): ObservationSet = metrics

  def withLineage(): GraphLineage[VD, ED] = this

  // TODO: add actual provenance graph that can be exported

  def pageRank(numIter: Int, dampingFactor: Double = 0.85): GraphLineage[Double, Unit] = {
    val (g, pregelMetrics) = LineagePageRank.run(
      graph, numIter, dampingFactor = dampingFactor
    )
    new GraphLineage[Double, Unit](g, pregelMetrics)
  }

  def bfs(sourceVertex: VertexId): GraphLineage[Long, ED] = {
    val (g, pregelMetrics) = LineageBFS.run(graph, sourceVertex)
    new GraphLineage[Long, ED](g, pregelMetrics)
  }

  def wcc(maxIterations: Int = Int.MaxValue): GraphLineage[VertexId, ED] = {
    val (g, pregelMetrics) = LineageWCC.run(graph, maxIterations = maxIterations)
    new GraphLineage[VertexId, ED](g, pregelMetrics)
  }

  def sssp(source: VertexId): GraphLineage[Double, Double] = {
    val (g, pregelMetrics) = LineageSSSP.run(graph, source)
    new GraphLineage[Double, Double](g, pregelMetrics)
  }

  def cdlp(): GraphLineage[VertexId, Unit] = {
    val (g, pregelMetrics) = LineageCDLP.run(graph)
    new GraphLineage[VertexId, Unit](g, pregelMetrics)
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
    new GraphLineage[VD, ED](g, ObservationSet())
  }
}
