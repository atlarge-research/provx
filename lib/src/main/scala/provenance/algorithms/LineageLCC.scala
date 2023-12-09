package lu.magalhaes.gilles.provxlib
package provenance.algorithms

import provenance.GraphLineage

import org.apache.spark.graphx.{EdgeContext, EdgeDirection, VertexId, VertexRDD}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala
import scala.reflect.ClassTag
import scala.collection.concurrent

object LineageLCC extends Serializable {

  def run[VD: ClassTag, ED: ClassTag](
      gl: GraphLineage[VD, ED]
  ): GraphLineage[Double, Unit] = {
    val canonicalGraph =
      gl.mapEdges(_ => 1).convertToCanonicalEdges(_ + _).cache()

    // Collect for each vertex a map of (neighbour, edge value) pairs in either
    // direction in the canonical graph
    val neighboursPerVertex: VertexRDD[concurrent.Map[VertexId, Int]] =
      canonicalGraph
        .collectEdges(EdgeDirection.Either)
        .mapValues((vid, edges) => {
          val m = new ConcurrentHashMap[VertexId, Int]().asScala
          edges
            .foreach(edge => m.putIfAbsent(edge.otherVertexId(vid), edge.attr))
          m
        })

    // Attach the neighbourhood maps as values to the canonical graph
    val neighbourGraph = canonicalGraph
      .outerJoinVertices(neighboursPerVertex)((_, _, neighboursOpt) =>
        neighboursOpt.getOrElse(new ConcurrentHashMap[VertexId, Int]().asScala)
      )
      .cache()
    // Unpersist the original canonical graph
    canonicalGraph.unpersist()

    // Define the edge-based "map" function
    def edgeToCounts(
        ctx: EdgeContext[
          scala.collection.concurrent.Map[VertexId, Int],
          Int,
          Long
        ]
    ): Unit = {
      var countSrc = 0L
      var countDst = 0L
      if (ctx.srcAttr.size < ctx.dstAttr.size) {
        val iter = ctx.srcAttr.iterator
        while (iter.hasNext) {
          val neighbourPair = iter.next()
          countSrc += ctx.dstAttr.getOrElse(neighbourPair._1, 0)
          if (ctx.dstAttr.contains(neighbourPair._1)) {
            countDst += neighbourPair._2
          }
        }
      } else {
        val iter = ctx.dstAttr.iterator
        while (iter.hasNext) {
          val neighbourPair = iter.next()
          countDst += ctx.srcAttr.getOrElse(neighbourPair._1, 0)
          if (ctx.srcAttr.contains(neighbourPair._1)) {
            countSrc += neighbourPair._2
          }
        }
      }
      ctx.sendToSrc(countSrc)
      ctx.sendToDst(countDst)
    }

    // Aggregate messages for all vertices in the map
    val triangles = neighbourGraph.aggregateMessages(
      edgeToCounts,
      (a: Long, b: Long) => a + b
    )
    val triangleCountGraph =
      neighbourGraph
        .outerJoinVertices(triangles)((_, _, triangleCountOpt) =>
          triangleCountOpt.getOrElse(0L) / 2
        )
        .cache()

    // Compute the number of neighbours each vertex has
    val neighbourCounts =
      neighbourGraph
        .collectNeighbors(EdgeDirection.Either)
        .mapValues(_.length)
        .cache()
    val lccGraph = triangleCountGraph
      .outerJoinVertices(neighbourCounts)(
        (_, triangleCount, neighbourCountOpt) => {
          val neighbourCount = neighbourCountOpt.getOrElse(0)
          if (neighbourCount < 2) 0.0
          else triangleCount.toDouble / neighbourCount / (neighbourCount - 1)
        }
      )
      .cache()

    // Materialize the result
    lccGraph.vertices.count()
    lccGraph.edges.count()

    // Unpersist the canonical graph
    canonicalGraph.unpersistVertices()
    canonicalGraph.edges.unpersist(blocking = false)

    // TODO: set metrics
    lccGraph.mapEdges(_ => ())
  }
}
