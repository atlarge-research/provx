package lu.magalhaes.gilles.provxlib
package lineage.lib

import lineage.{LineagePregel, PregelMetrics}

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/** Connected components algorithm. */
object LineageWCC {
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], maxIterations: Int = Int.MaxValue):
    (Graph[VertexId, ED], PregelMetrics) =
  {
    require(maxIterations > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    val ccGraph = graph.mapVertices { case (vid, _) => vid }

    def sendMessage(edge: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, VertexId)] = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }

    val initialMessage = Long.MaxValue
    val pregelGraph = LineagePregel(ccGraph, initialMessage,
      maxIterations, EdgeDirection.Out)(
      vprog = (id, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))
    ccGraph.unpersist()
    pregelGraph
  } // end of connectedComponents
}

