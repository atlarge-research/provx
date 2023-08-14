package lu.magalhaes.gilles.provxlib
package lineage

import lineage.algorithms._
import lineage.metrics.ObservationSet

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

class GraphLineage[VD: ClassTag, ED: ClassTag](
    val graph: Graph[VD, ED], val metrics: ObservationSet = ObservationSet(),
    val captureFilter: CaptureQuery = new CaptureQuery(),
    val annotations: ArrayBuffer[String] = ArrayBuffer.empty) {

  val id: Int = LineageContext.newGLId(this)

  // TODO: move this to only apply on Graph instead of also GraphLineage to differentiate sources
  def withLineage(): GraphLineage[VD, ED] = {
    this.annotations += "src"
    this
  }

  // TODO: wrap trace here to allow for multiple granularity levels
  def pageRank(numIter: Int, dampingFactor: Double = 0.85): GraphLineage[Double, Unit] = trace(Algorithm("pageRank")) {
    LineagePageRank.run(this, numIter, dampingFactor = dampingFactor)
  }

  def bfs(sourceVertex: VertexId): GraphLineage[Long, ED] = trace(Algorithm("bfs")) {
    LineageBFS.run(this, sourceVertex)
  }

  def wcc(maxIterations: Int = Int.MaxValue): GraphLineage[VertexId, ED] = trace(Algorithm("wcc")) {
    LineageWCC.run(this, maxIterations = maxIterations)
  }

  def sssp(source: VertexId): GraphLineage[Double, Double] = trace(Algorithm("sssp")) {
    LineageSSSP.run(this, source)
  }

  // TODO: broken
  def cdlp(): GraphLineage[VertexId, Unit] = trace(Algorithm("cdlp")) {
    LineageCDLP.run(this)
  }

  // TODO: broken
  def lcc(): GraphLineage[Double, Unit] = trace(Algorithm("lcc")) {
    LineageLCC.run(this)
  }

//  private var captureFilter: CaptureQuery = new CaptureQuery(this)

  def capture(captureFilter: CaptureQuery => CaptureQuery): GraphLineage[VD, ED] = {
    val c = captureFilter(new CaptureQuery())
    new GraphLineage(graph, metrics, captureFilter = c)
  }

  // graph query operations
  def query(): ProvenanceQuery = {
    new ProvenanceQuery(this)
  }

  // GraphOps and Graph GraphX interfaces
  val vertices: VertexRDD[VD] = graph.vertices
  val edges: EdgeRDD[ED] = graph.edges

  val numVertices: VertexId = graph.numVertices
  val outDegrees: VertexRDD[Int] = graph.outDegrees

  def mapVertices[VD2: ClassTag](f: (VertexId, VD) => VD2)(implicit eq: VD =:= VD2 = null): GraphLineage[VD2, ED] =
    trace(Operation("mapVertices")) { new GraphLineage(graph.mapVertices(f)) }

  def joinVertices[U: ClassTag](table: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, U) => VD) : GraphLineage[VD, ED] =
    trace(Operation("joinVertices")) { new GraphLineage(graph.joinVertices(table)(mapFunc)) }

  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): GraphLineage[VD, ED2] = trace(Operation("mapEdges")) {
    new GraphLineage(graph.mapEdges(map))
  }

  def cache(): GraphLineage[VD, ED] = trace(Operation("cache")) {
    graph.cache()
  }

  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])
      (mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null) : GraphLineage[VD2, ED] =
    trace(Operation("outerJoinVertices")) { new GraphLineage(graph.outerJoinVertices(other)(mapFunc)) }

  def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): GraphLineage[VD, ED2] =
    trace(Operation("mapTriplets")) { new GraphLineage(graph.mapTriplets(map)) }

  def unpersist(blocking: Boolean = false): GraphLineage[VD, ED] =
    trace(Operation("unpersist")) { new GraphLineage(graph.unpersist(blocking)) }

  def unpersistVertices(blocking: Boolean = false): GraphLineage[VD, ED] =
    trace(Operation("unpersistVertices")) { new GraphLineage(graph.unpersistVertices(blocking)) }

  def convertToCanonicalEdges(mergeFunc: (ED, ED) => ED = (e1, e2) => e1): GraphLineage[VD, ED] =
    trace(Operation("convertToCanonicalEdges")) { new GraphLineage(graph.convertToCanonicalEdges(mergeFunc)) }

  def collectEdges(edgeDirection: EdgeDirection): VertexRDD[Array[Edge[ED]]] = {
    graph.collectEdges(edgeDirection)
  }

  def collectNeighbors(edgeDirection: EdgeDirection): VertexRDD[Array[(VertexId, VD)]] = {
    graph.collectNeighbors(edgeDirection)
  }

  def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.All)
      : VertexRDD[A] = {
    graph.aggregateMessages(sendMsg, mergeMsg, tripletFields)
  }

  def trace[VD1: ClassTag, VD2: ClassTag](event: EventType)(f: => GraphLineage[VD1, VD2]): GraphLineage[VD1, VD2] = {
    Utils.trace(this, event)(f)
  }
}

object GraphLineage {
  // From non-provenance world to provenance world
  def apply[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]): GraphLineage[VD, ED] = {
    val res = new GraphLineage(g)
//    LineageContext.graph.add(ProvenanceGraph.Relation(ProvenanceSource(res))
    res
  }

  implicit def graphToGraphLineage[VD: ClassTag, ED: ClassTag]
      (g: Graph[VD, ED]): GraphLineage[VD, ED] = GraphLineage.apply[VD, ED](g)
}
