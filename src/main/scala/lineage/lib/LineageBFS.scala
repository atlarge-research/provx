package lu.magalhaes.gilles.provxlib
package lineage.lib

import lineage.{LineagePregel, PregelMetrics}

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

object LineageBFS {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], source: VertexId):
      (Graph[Long, ED], PregelMetrics) = {
    val bfsGraph = graph
      .mapVertices((vid, _) => {
        if (vid == source) {
          0L
        } else {
          Long.MaxValue
        }
      }).cache()

    def vertexProgram(id: VertexId, oldValue: Long, message: Long): Long = {
      math.min(oldValue, message)
    }

    def sendMessage(edge: EdgeTriplet[Long, ED]): Iterator[(VertexId, VertexId)] = {
      if (edge.srcAttr < Long.MaxValue && edge.srcAttr + 1L < edge.dstAttr) {
        Iterator((edge.dstId, edge.srcAttr + 1L))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Long, b: Long): Long = math.min(a, b)

    val initialMessage = Long.MaxValue

    LineagePregel(
      bfsGraph, initialMessage, activeDirection = EdgeDirection.Out
    )(
      vertexProgram, sendMessage, messageCombiner
    )
  }
}
