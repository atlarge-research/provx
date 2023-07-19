package lu.magalhaes.gilles.provxlib
package lineage

import lineage.algorithms._
import lineage.hooks.HooksRegistry
import lineage.metrics.ObservationSet

import org.apache.spark.graphx.{Edge, EdgeContext, EdgeDirection, EdgeRDD, EdgeTriplet, Graph, TripletFields, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

class GraphLineage[VD: ClassTag, ED: ClassTag](
    val graph: Graph[VD, ED], val lineageContext: LineageLocalContext,
    val annotations: ArrayBuffer[String] = ArrayBuffer.empty) {

  val id: Int = LineageContext.newGLId(this)

  private val hooksRegistry = new HooksRegistry()

  private var _metrics = ObservationSet()

  def setMetrics(set: ObservationSet): Unit = {
    _metrics = set
  }

  def metrics: ObservationSet = _metrics

  def withLineage(): GraphLineage[VD, ED] = {
    this.annotations += "src"
    this
  }

  def pageRank(numIter: Int, dampingFactor: Double = 0.85): GraphLineage[Double, Unit] = withProvenance(chain = false) {
    val res = LineagePageRank.run(this, numIter, dampingFactor = dampingFactor)
    res.annotations += "pageRankResult"
    res
  }

  def bfs(sourceVertex: VertexId): GraphLineage[Long, ED] = withProvenance(chain = false) {
    val res = LineageBFS.run(this, sourceVertex)
    res.annotations += "bfsResult"
    res
  }

  def wcc(maxIterations: Int = Int.MaxValue): GraphLineage[VertexId, ED] = withProvenance(chain = false) {
    LineageWCC.run(this, maxIterations = maxIterations)
  }

  def sssp(source: VertexId): GraphLineage[Double, Double] = withProvenance(chain = false) {
    LineageSSSP.run(this, source)
  }

  // TODO: broken
  def cdlp(): GraphLineage[VertexId, Unit] = withProvenance(chain = false) {
    LineageCDLP.run(this)
  }

  // TODO: broken
  def lcc(): GraphLineage[Double, Unit] = withProvenance(chain = false) {
    LineageLCC.run(this)
  }

  def next(): Seq[GraphLineage[_, _]] = {
    LineageContext.graph.pairs.filter(x => x._1.id == id).map(_._2)
  }

  // GraphOps and Graph GraphX interfaces

  def vertices: VertexRDD[VD] = graph.vertices
  def edges: EdgeRDD[ED] = graph.edges

  def numVertices: VertexId = graph.numVertices
  def outDegrees: VertexRDD[Int] = graph.outDegrees

  def mapVertices[VD2: ClassTag]
      (f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): GraphLineage[VD2, ED] = withProvenance() {
    val res = new GraphLineage(graph.mapVertices(f), lineageContext)
    res.annotations += sourcecode.Name()
    res
  }

  def joinVertices[U: ClassTag]
      (table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD) : GraphLineage[VD, ED] = withProvenance() {
    val res = new GraphLineage(graph.joinVertices(table)(mapFunc), lineageContext)
    res.annotations += sourcecode.Name()
    res
  }

  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): GraphLineage[VD, ED2] = withProvenance() {
    val res = new GraphLineage(graph.mapEdges(map), lineageContext)
    res.annotations += sourcecode.Name()
    res
  }

  def cache(): GraphLineage[VD, ED] = withProvenance() {
    graph.cache()
  }

  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
     (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
      : GraphLineage[VD2, ED] = withProvenance() {
    val res = new GraphLineage(graph.outerJoinVertices(other)(mapFunc), lineageContext)
    res.annotations += sourcecode.Name()
    res
  }

  def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): GraphLineage[VD, ED2] = withProvenance() {
    val res = new GraphLineage(graph.mapTriplets(map), lineageContext)
    res.annotations += sourcecode.Name()
    res
  }

  def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.All)
        : VertexRDD[A] = {
    graph.aggregateMessages(sendMsg, mergeMsg, tripletFields)
  }

  def unpersist(blocking: Boolean = false): GraphLineage[VD, ED] = {
    val res = new GraphLineage(graph.unpersist(blocking), lineageContext)
    res.annotations += sourcecode.Name()
    res
  }

  def unpersistVertices(blocking: Boolean = false): GraphLineage[VD, ED] = {
    val res = new GraphLineage(graph.unpersistVertices(blocking), lineageContext)
    res.annotations += sourcecode.Name()
    res
  }

  def convertToCanonicalEdges(
      mergeFunc: (ED, ED) => ED = (e1, e2) => e1): GraphLineage[VD, ED] = withProvenance() {
    val res = new GraphLineage(graph.convertToCanonicalEdges(mergeFunc), lineageContext)
    res.annotations += sourcecode.Name()
    res
  }

  def collectEdges(edgeDirection: EdgeDirection): VertexRDD[Array[Edge[ED]]] = {
    graph.collectEdges(edgeDirection)
  }

  def collectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, VD)]] = {
    graph.collectNeighbors(edgeDirection)
  }

  // Helpers
  private def withProvenance[VD1: ClassTag, VD2: ClassTag](chain: Boolean = true)(f: => GraphLineage[VD1, VD2]): GraphLineage[VD1, VD2] = {
    val res = f
    if (chain) {
      LineageContext.graph.chain(this, res)
    }
    res
  }
}

object GraphLineage {
  def apply[VD: ClassTag, ED: ClassTag](g: GraphLineage[VD, ED]): GraphLineage[VD, ED] = {
    val res = new GraphLineage(g.graph, new LineageLocalContext(g.vertices.sparkContext))
    LineageContext.graph.chain(g, res)
    res
  }


  implicit def graphToGraphLineage[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphLineage[VD, ED] = {
    new GraphLineage[VD, ED](g, new LineageLocalContext(g.vertices.sparkContext))
  }
}
