package lu.magalhaes.gilles.provxlib
package provenance

import provenance.algorithms._
import provenance.events._
import provenance.metrics.ObservationSet

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions
import scala.reflect.ClassTag

class GraphLineage[VD: ClassTag, ED: ClassTag](
    val graph: Graph[VD, ED],
    override val metrics: ObservationSet = ObservationSet()
) extends ProvenanceGraphNode {

  val id: Int = ProvenanceContext.newGLId(this)

  def withLineage(sparkSession: SparkSession): GraphLineage[VD, ED] = {
    ProvenanceContext.sparkContext = Some(sparkSession)
    this
  }

  def pageRank(
      numIter: Int,
      dampingFactor: Double = 0.85
  ): GraphLineage[Double, Unit] = trace(PageRank(numIter, dampingFactor)) {
    LineagePageRank.run(this, numIter, dampingFactor = dampingFactor)
  }

  def bfs(sourceVertex: VertexId): GraphLineage[Long, ED] =
    trace(BFS(sourceVertex)) {
      LineageBFS.run(this, sourceVertex)
    }

  def wcc(maxIterations: Int = Int.MaxValue): GraphLineage[VertexId, ED] =
    trace(WCC(maxIterations)) {
      LineageWCC.run(this, maxIterations = maxIterations)
    }

  def sssp(source: VertexId): GraphLineage[Double, Double] =
    trace(SSSP(source)) {
      LineageSSSP.run(this, source)
    }

  // Not implemented since causes Spark to hang
  def cdlp(): GraphLineage[VertexId, Unit] = ???
  //  trace(Algorithm("cdlp")) {
  //    LineageCDLP.run(this)
  //  }

  // Not implemented since causes Spark to hang
  def lcc(): GraphLineage[Double, Unit] = ???
  //  trace(LCC()) {
  //    LineageLCC.run(this)
  //  }

  // GraphOps and Graph GraphX interfaces
  val vertices: VertexRDD[VD] = graph.vertices
  val edges: EdgeRDD[ED] = graph.edges

  def numVertices: VertexId = graph.numVertices
  def outDegrees: VertexRDD[Int] = graph.outDegrees

  def mapVertices[VD2: ClassTag](
      f: (VertexId, VD) => VD2
  )(implicit eq: VD =:= VD2 = null): GraphLineage[VD2, ED] =
    trace(Operation("mapVertices")) {
      new GraphLineage(graph.mapVertices(f))
    }

  def joinVertices[U: ClassTag](
      table: RDD[(VertexId, U)]
  )(mapFunc: (VertexId, VD, U) => VD): GraphLineage[VD, ED] =
    trace(Operation("joinVertices")) {
      new GraphLineage(
        graph.joinVertices(table)(mapFunc)
      )
    }

  def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): GraphLineage[VD, ED2] =
    trace(Operation("mapEdges")) {
      new GraphLineage(graph.mapEdges(map))
    }

  def cache(): GraphLineage[VD, ED] = trace(Operation("cache")) {
    new GraphLineage(graph.cache())
  }

  def outerJoinVertices[U: ClassTag, VD2: ClassTag](other: RDD[(VertexId, U)])(
      mapFunc: (VertexId, VD, Option[U]) => VD2
  )(implicit eq: VD =:= VD2 = null): GraphLineage[VD2, ED] =
    trace(Operation("outerJoinVertices")) {
      new GraphLineage(
        graph.outerJoinVertices(other)(mapFunc)
      )
    }

  def mapTriplets[ED2: ClassTag](
      map: EdgeTriplet[VD, ED] => ED2
  ): GraphLineage[VD, ED2] =
    trace(Operation("mapTriplets")) {
      new GraphLineage(graph.mapTriplets(map))
    }

  def unpersist(blocking: Boolean = false): GraphLineage[VD, ED] =
    trace(Operation("unpersist")) {
      new GraphLineage(graph.unpersist(blocking))
    }

  def unpersistVertices(blocking: Boolean = false): GraphLineage[VD, ED] =
    trace(Operation("unpersistVertices")) {
      new GraphLineage(
        graph.unpersistVertices(blocking)
      )
    }

  def removeSelfEdges(): GraphLineage[VD, ED] = {
    trace(Operation("removeSelfEdges")) {
      new GraphLineage(
        graph.subgraph(epred = e => e.srcId != e.dstId)
      )
    }
  }

  def convertToCanonicalEdges(
      mergeFunc: (ED, ED) => ED = (e1, e2) => e1
  ): GraphLineage[VD, ED] =
    trace(Operation("convertToCanonicalEdges")) {
      new GraphLineage(
        graph.convertToCanonicalEdges(mergeFunc)
      )
    }

  def collectEdges(edgeDirection: EdgeDirection): VertexRDD[Array[Edge[ED]]] = {
    graph.collectEdges(edgeDirection)
  }

  def collectNeighbors(
      edgeDirection: EdgeDirection
  ): VertexRDD[Array[(VertexId, VD)]] = {
    graph.collectNeighbors(edgeDirection)
  }

  def aggregateMessages[A: ClassTag](
      sendMsg: EdgeContext[VD, ED, A] => Unit,
      mergeMsg: (A, A) => A,
      tripletFields: TripletFields = TripletFields.All
  ): VertexRDD[A] = {
    graph.aggregateMessages(sendMsg, mergeMsg, tripletFields)
  }

  override def toString: String = s"G${id}"

  def trace[VD1: ClassTag, VD2: ClassTag](
      event: EventType
  )(f: => GraphLineage[VD1, VD2]): GraphLineage[VD1, VD2] = {
    Pipeline.trace(this, event)(f)
  }
}

object GraphLineage {
  // From non-provenance world to provenance world
  def apply[VD: ClassTag, ED: ClassTag](
      g: Graph[VD, ED]
  ): GraphLineage[VD, ED] = {
    val res = new GraphLineage(g)
    res
  }

  implicit def graphToGraphLineage[VD: ClassTag, ED: ClassTag](
      g: Graph[VD, ED]
  ): GraphLineage[VD, ED] = GraphLineage.apply[VD, ED](g)
}
