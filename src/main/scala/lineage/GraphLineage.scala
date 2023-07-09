package lu.magalhaes.gilles.provxlib
package lineage

import lineage.algorithms._
import lineage.metrics.ObservationSet

import lu.magalhaes.gilles.provxlib.lineage.hooks.HooksRegistry
import org.apache.spark.graphx.{Graph, VertexId}

import scala.language.implicitConversions
import scala.reflect.ClassTag

class GraphLineage[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], val lineageContext: LineageLocalContext, metrics: ObservationSet) {

  private val hooksRegistry = new HooksRegistry()

  // TODO: add actual provenance graph that can be exported
  private val provenanceGraph = new ProvenanceGraph()

  def getGraph(): Graph[VD, ED] = graph

  def getMetrics(): ObservationSet = metrics

  def withLineage(): GraphLineage[VD, ED] = this

  def pageRank(numIter: Int, dampingFactor: Double = 0.85): GraphLineage[Double, Unit] = {
    val (g, pregelMetrics) = LineagePageRank.run(
      graph, numIter, lineageContext, dampingFactor = dampingFactor
    )
    new GraphLineage[Double, Unit](g, lineageContext, pregelMetrics)
  }

  def bfs(sourceVertex: VertexId): GraphLineage[Long, ED] = {
    val (g, pregelMetrics) = LineageBFS.run(graph, lineageContext, sourceVertex)
    new GraphLineage[Long, ED](g, lineageContext, pregelMetrics)
  }

  def wcc(maxIterations: Int = Int.MaxValue): GraphLineage[VertexId, ED] = {
    val (g, pregelMetrics) = LineageWCC.run(graph, lineageContext, maxIterations = maxIterations)
    new GraphLineage[VertexId, ED](g, lineageContext, pregelMetrics)
  }

  def sssp(source: VertexId): GraphLineage[Double, Double] = {
    val (g, pregelMetrics) = LineageSSSP.run(graph, lineageContext, source)
    new GraphLineage[Double, Double](g, lineageContext, pregelMetrics)
  }

  // TODO: broken
  def cdlp(): GraphLineage[VertexId, Unit] = {
    val (g, pregelMetrics) = LineageCDLP.run(graph, lineageContext)
    new GraphLineage[VertexId, Unit](g, lineageContext, pregelMetrics)
  }

  // TODO: broken
  def lcc(): Graph[Double, Unit] = {
    LineageLCC.run(graph)
  }
}

object GraphLineage {

  implicit def graphToGraphLineage[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphLineage[VD, ED] = {
    new GraphLineage[VD, ED](g, new LineageLocalContext(g.vertices.sparkContext), ObservationSet())
  }
}
