package lu.magalhaes.gilles.provxlib
package lineage.lib

import lineage.{LineagePregel, PregelMetrics}

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

object LineageSSSP {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], source: VertexId):
      (Graph[Double, Double], PregelMetrics) = {
    val ssspGraph = graph.mapVertices((vid, _) => {
        if (vid == source) {
          0.0
        } else {
          Double.PositiveInfinity
        }
      })
      .mapEdges(x => x.attr.toString.toDouble)
      .cache()

    def vertexProgram(id: VertexId, oldValue: Double, message: Double): Double = {
      math.min(oldValue, message)
    }

    def sendMessage(edgeData: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = {
      if (edgeData.srcAttr + edgeData.attr < edgeData.dstAttr) {
        Iterator((edgeData.dstId, edgeData.srcAttr + edgeData.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = {
      math.min(a, b)
    }

    val initialMessage = Double.PositiveInfinity

    LineagePregel(
      ssspGraph, initialMessage, activeDirection = EdgeDirection.Out
    )(
      vertexProgram, sendMessage, messageCombiner
    )
  }

}
