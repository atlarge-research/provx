package lu.magalhaes.gilles.provxlib
package lineage

import lineage.algorithms._
import lineage.hooks.HooksRegistry
import lineage.metrics.ObservationSet

import org.apache.spark.graphx.{Graph, VertexId}

import scala.language.implicitConversions
import scala.reflect.ClassTag

class GraphLineage[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], val lineageContext: LineageLocalContext) {

  val id: Int = LineageContext.newGLId()

  private val hooksRegistry = new HooksRegistry()

  // TODO: add actual provenance graph that can be exported
  private val provenanceGraph = new ProvenanceGraph()

  def getGraph(): Graph[VD, ED] = graph

  private var metrics = ObservationSet()

  def setMetrics(set: ObservationSet): Unit = {
    metrics = set
  }

  def getMetrics(): ObservationSet = metrics

  def withLineage(): GraphLineage[VD, ED] = this

  def pageRank(numIter: Int, dampingFactor: Double = 0.85): GraphLineage[Double, Unit] = {
    LineagePageRank.run(
      this, numIter, dampingFactor = dampingFactor
    )
  }

  def bfs(sourceVertex: VertexId): GraphLineage[Long, ED] = {
    LineageBFS.run(this, sourceVertex)
  }

  def wcc(maxIterations: Int = Int.MaxValue): GraphLineage[VertexId, ED] = {
    LineageWCC.run(this, maxIterations = maxIterations)
  }

  def sssp(source: VertexId): GraphLineage[Double, Double] = {
    LineageSSSP.run(this, source)
  }

  // TODO: broken
  def cdlp(): GraphLineage[VertexId, Unit] = {
    LineageCDLP.run(this)
  }

  // TODO: broken
  def lcc(): GraphLineage[Double, Unit] = {
    LineageLCC.run(this)
  }
}

object GraphLineage {

  implicit def graphToGraphLineage[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphLineage[VD, ED] = {
    new GraphLineage[VD, ED](g, new LineageLocalContext(g.vertices.sparkContext))
  }
}
