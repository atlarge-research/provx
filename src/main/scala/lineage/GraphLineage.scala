package lu.magalhaes.gilles.provxlib
package lineage

import lineage.algorithms._
import lineage.hooks.HooksRegistry
import lineage.metrics.ObservationSet

import org.apache.spark.graphx.{Edge, EdgeContext, EdgeDirection, EdgeRDD, EdgeTriplet, Graph, TripletFields, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

class GraphLineage[VD: ClassTag, ED: ClassTag](val graph: Graph[VD, ED], val lineageContext: LineageLocalContext) {

  val id: Int = LineageContext.newGLId(this)

  private val hooksRegistry = new HooksRegistry()

  private var _metrics = ObservationSet()

  def setMetrics(set: ObservationSet): Unit = {
    _metrics = set
  }

  def metrics: ObservationSet = _metrics

  def withLineage(): GraphLineage[VD, ED] = this

  private def withProvenance[VD1: ClassTag, VD2: ClassTag](f: => GraphLineage[VD1, VD2]): GraphLineage[VD1, VD2] = {
    val res = f
    LineageContext.graph.chain(this, res)
    res
  }

  def pageRank(numIter: Int, dampingFactor: Double = 0.85): GraphLineage[Double, Unit] = withProvenance {
    LineagePageRank.run(this, numIter, dampingFactor = dampingFactor)
  }

  def bfs(sourceVertex: VertexId): GraphLineage[Long, ED] = withProvenance {
    LineageBFS.run(this, sourceVertex)
  }

  def wcc(maxIterations: Int = Int.MaxValue): GraphLineage[VertexId, ED] = withProvenance {
    LineageWCC.run(this, maxIterations = maxIterations)
  }

  def sssp(source: VertexId): GraphLineage[Double, Double] = withProvenance {
    LineageSSSP.run(this, source)
  }

  // TODO: broken
  def cdlp(): GraphLineage[VertexId, Unit] = withProvenance {
    LineageCDLP.run(this)
  }

  // TODO: broken
  def lcc(): GraphLineage[Double, Unit] = withProvenance {
    LineageLCC.run(this)
  }

  def vertices: VertexRDD[VD] = graph.vertices
  def edges: EdgeRDD[ED] = graph.edges

  def numVertices: VertexId = graph.numVertices
  def outDegrees: VertexRDD[Int] = graph.outDegrees

  def mapVertices[VD2: ClassTag]
      (f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): GraphLineage[VD2, ED] = withProvenance {
    new GraphLineage(graph.mapVertices(f), lineageContext)
  }

  def joinVertices[U: ClassTag]
      (table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD) : GraphLineage[VD, ED] = withProvenance {
    new GraphLineage(graph.joinVertices(table)(mapFunc), lineageContext)
  }

  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): GraphLineage[VD, ED2] = withProvenance {
    new GraphLineage(graph.mapEdges(map), lineageContext)
  }

  def cache(): GraphLineage[VD, ED] = {
    graph.cache()
  }

  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
     (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
      : GraphLineage[VD2, ED] = withProvenance {
    new GraphLineage(graph.outerJoinVertices(other)(mapFunc), lineageContext)
  }

  def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): GraphLineage[VD, ED2] = withProvenance {
    new GraphLineage(graph.mapTriplets(map), lineageContext)
  }

  def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.All)
        : VertexRDD[A] = {
    graph.aggregateMessages(sendMsg, mergeMsg, tripletFields)
  }

  def unpersist(blocking: Boolean = false): GraphLineage[VD, ED] = withProvenance {
    new GraphLineage(graph.unpersist(blocking), lineageContext)
  }

  def unpersistVertices(blocking: Boolean = false): GraphLineage[VD, ED] = withProvenance {
    new GraphLineage(graph.unpersistVertices(blocking), lineageContext)
  }

  def convertToCanonicalEdges(
      mergeFunc: (ED, ED) => ED = (e1, e2) => e1): GraphLineage[VD, ED] = withProvenance {
    new GraphLineage(graph.convertToCanonicalEdges(mergeFunc), lineageContext)
  }

  def collectEdges(edgeDirection: EdgeDirection): VertexRDD[Array[Edge[ED]]] = {
    graph.collectEdges(edgeDirection)
  }

  def collectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, VD)]] = {
    graph.collectNeighbors(edgeDirection)
  }
}

object GraphLineage {

  implicit def graphToGraphLineage[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphLineage[VD, ED] = {
    new GraphLineage[VD, ED](g, new LineageLocalContext(g.vertices.sparkContext))
  }
}
