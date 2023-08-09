package lu.magalhaes.gilles.provxlib
package lineage

import lineage.algorithms._
import lineage.metrics.ObservationSet

import lu.magalhaes.gilles.provxlib.lineage.hooks.{CounterHook, PregelHook, TimeHook}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

class GraphLineage[VD: ClassTag, ED: ClassTag](
    val graph: Graph[VD, ED], val metrics: ObservationSet = ObservationSet(),
    val annotations: ArrayBuffer[String] = ArrayBuffer.empty) {

  val id: Int = LineageContext.newGLId(this)

  def withLineage(): GraphLineage[VD, ED] = {
    this.annotations += "src"
    this
  }

  // TODO: wrap trace here to allow for multiple granularity levels
  def pageRank(numIter: Int, dampingFactor: Double = 0.85): GraphLineage[Double, Unit] = trace("pageRank") {
    val res = LineagePageRank.run(this, numIter, dampingFactor = dampingFactor)
    res.annotations += "pageRankResult"
    res
  }

  def bfs(sourceVertex: VertexId): GraphLineage[Long, ED] = trace() {
    val res = LineageBFS.run(this, sourceVertex)
    res.annotations += "bfsResult"
    res
  }

  def wcc(maxIterations: Int = Int.MaxValue): GraphLineage[VertexId, ED] = trace() {
    LineageWCC.run(this, maxIterations = maxIterations)
  }

  def sssp(source: VertexId): GraphLineage[Double, Double] = trace() {
    LineageSSSP.run(this, source)
  }

  // TODO: broken
  def cdlp(): GraphLineage[VertexId, Unit] = trace() {
    LineageCDLP.run(this)
  }

  // TODO: broken
  def lcc(): GraphLineage[Double, Unit] = trace() {
    LineageLCC.run(this)
  }

  // graph query operations

  def next(): Seq[GraphLineage[_, _]] = {
    LineageContext.graph.pairs.filter(x => x._1.id == id).map(_._2)
  }

  def previous(): Seq[GraphLineage[_, _]] = {
    LineageContext.graph.pairs.filter(x => x._2.id == id).map(_._1)
  }

  // GraphOps and Graph GraphX interfaces
  val vertices: VertexRDD[VD] = graph.vertices
  val edges: EdgeRDD[ED] = graph.edges

  val numVertices: VertexId = graph.numVertices
  val outDegrees: VertexRDD[Int] = graph.outDegrees

  def mapVertices[VD2: ClassTag]
      (f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): GraphLineage[VD2, ED] = trace() {
    val res = new GraphLineage(graph.mapVertices(f))
    res.annotations += sourcecode.Name()
    res
  }

  def joinVertices[U: ClassTag]
      (table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD) : GraphLineage[VD, ED] = trace() {
    val res = new GraphLineage(graph.joinVertices(table)(mapFunc))
    res.annotations += sourcecode.Name()
    res
  }

  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): GraphLineage[VD, ED2] = trace() {
    val res = new GraphLineage(graph.mapEdges(map))
    res.annotations += sourcecode.Name()
    res
  }

  def cache(): GraphLineage[VD, ED] = trace() {
    graph.cache()
  }

  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
    (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null)
      : GraphLineage[VD2, ED] = trace() {
    val res = new GraphLineage(graph.outerJoinVertices(other)(mapFunc))
    res.annotations += sourcecode.Name()
    res
  }

  def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): GraphLineage[VD, ED2] = trace() {
    val res = new GraphLineage(graph.mapTriplets(map))
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
    val res = new GraphLineage(graph.unpersist(blocking))
    res.annotations += sourcecode.Name()
    res
  }

  def unpersistVertices(blocking: Boolean = false): GraphLineage[VD, ED] = {
    val res = new GraphLineage(graph.unpersistVertices(blocking))
    res.annotations += sourcecode.Name()
    res
  }

  def convertToCanonicalEdges(mergeFunc: (ED, ED) => ED = (e1, e2) => e1): GraphLineage[VD, ED] = trace() {
    val res = new GraphLineage(graph.convertToCanonicalEdges(mergeFunc))
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
  private def trace[VD1: ClassTag, VD2: ClassTag](name: String = "")(f: => GraphLineage[VD1, VD2]): GraphLineage[VD1, VD2] = {
    LineageContext.hooks.handlePre(name, this)
    val res = f
    LineageContext.hooks.handlePost(name, res)
    LineageContext.graph.chain(this, res)
    res
  }
}

object GraphLineage {
  def apply[VD: ClassTag, ED: ClassTag](g: GraphLineage[VD, ED], metrics: ObservationSet = ObservationSet()): GraphLineage[VD, ED] = {
    val res = new GraphLineage(g.graph, metrics = metrics)
    LineageContext.graph.chain(g, res)
    res
  }

  implicit def graphToGraphLineage[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphLineage[VD, ED] = new GraphLineage[VD, ED](g)
}
