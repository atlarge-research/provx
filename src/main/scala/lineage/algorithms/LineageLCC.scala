package lu.magalhaes.gilles.provxlib
package lineage.algorithms

import lineage.GraphLineage

import org.apache.spark.graphx.{EdgeContext, EdgeDirection, VertexId}

import scala.reflect.ClassTag

object LineageLCC {

  def run[VD: ClassTag, ED: ClassTag](gl: GraphLineage[VD, ED]): GraphLineage[Double, Unit] = {
    val graph = gl.getGraph()

    // Deduplicate the edges to ensure that every pair of connected vertices is
    // compared exactly once. The value of an edge represents if the edge is
    // unidirectional (1) or bidirectional (2) in the input graph.
    val canonicalGraph = graph.mapEdges(_ => 1).convertToCanonicalEdges(_ + _).cache()

    // Collect for each vertex a map of (neighbour, edge value) pairs in either
    // direction in the canonical graph
    val neighboursPerVertex = canonicalGraph.collectEdges(EdgeDirection.Either)
      .mapValues((vid, edges) => edges.map(edge => (edge.otherVertexId(vid), edge.attr)).toMap)

    // Attach the neighbourhood maps as values to the canonical graph
    val neighbourGraph = canonicalGraph.outerJoinVertices(neighboursPerVertex)(
      (_, _, neighboursOpt) => neighboursOpt.getOrElse(Map[VertexId, Int]())
    ).cache()
    // Unpersist the original canonical graph
    canonicalGraph.unpersist(blocking = false)

    // Define the edge-based "map" function
    def edgeToCounts(ctx: EdgeContext[Map[VertexId, Int], Int, Long]) = {
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
    val triangles = neighbourGraph.aggregateMessages(edgeToCounts, (a: Long, b: Long) => a + b)
    val triangleCountGraph = neighbourGraph.outerJoinVertices(triangles)(
      (_, _, triangleCountOpt) => triangleCountOpt.getOrElse(0L) / 2)

    // Compute the number of neighbours each vertex has
    val neighbourCounts = neighbourGraph.collectNeighbors(EdgeDirection.Either).mapValues(_.length)
    val lccGraph = triangleCountGraph.outerJoinVertices(neighbourCounts)(
      (_, triangleCount, neighbourCountOpt) => {
        val neighbourCount = neighbourCountOpt.getOrElse(0)
        if (neighbourCount < 2) 0.0
        else triangleCount.toDouble / neighbourCount / (neighbourCount - 1)
      }
    ).cache()

    // Materialize the result
    lccGraph.vertices.count()
    lccGraph.edges.count()

    // Unpersist the canonical graph
    canonicalGraph.unpersistVertices(blocking = false)
    canonicalGraph.edges.unpersist(blocking = false)

    // TODO: set metrics
    new GraphLineage[Double, Unit](lccGraph.mapEdges(_ => Unit), gl.lineageContext)
  }
}
