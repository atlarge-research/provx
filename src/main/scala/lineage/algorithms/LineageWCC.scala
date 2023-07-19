package lu.magalhaes.gilles.provxlib
package lineage.algorithms

import lineage.{GraphLineage, LineageLocalContext, LineagePregel}
import lineage.metrics.ObservationSet

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/** Connected components algorithm. */
object LineageWCC {
  def run[VD: ClassTag, ED: ClassTag](gl: GraphLineage[VD, ED], maxIterations: Int = Int.MaxValue):
    GraphLineage[VertexId, ED] =
  {
    require(maxIterations > 0, s"Maximum of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    def sendMessage(edge: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, VertexId)] = {
      if (edge.srcAttr < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr))
      } else if (edge.srcAttr > edge.dstAttr) {
        Iterator((edge.srcId, edge.dstAttr))
      } else {
        Iterator.empty
      }
    }

    val ccGraph = gl.mapVertices { case (vid, _) => vid }
    val initialMessage = Long.MaxValue
    val pregelGraph = LineagePregel(
      ccGraph, initialMessage,
      maxIterations, EdgeDirection.Out)(
      vprog = (_, attr, msg) => math.min(attr, msg),
      sendMsg = sendMessage,
      mergeMsg = (a, b) => math.min(a, b))
    ccGraph.unpersist()
    pregelGraph
  }
}
