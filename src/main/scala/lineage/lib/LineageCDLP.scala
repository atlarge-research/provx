package lu.magalhaes.gilles.provxlib
package lineage.lib

import lineage.{LineagePregel, PregelMetrics}

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, Graph, VertexId}

import scala.reflect.ClassTag

object LineageCDLP {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]):
    (Graph[VertexId, Unit], PregelMetrics) =
  {
    val cdlpGraph = graph
      .mapVertices((vid, _) => vid)
      .mapEdges(_ => ())

    def vertexProgram(vid: VertexId, vertexData: VertexId, messageData: Map[VertexId, Long]):
      VertexId =
    {
      messageData.fold((vertexData, 0L))((a, b) =>
        if (a._2 > b._2 || (a._2 == b._2 && a._1 < b._1)) a
        else b
      )._1
    }

    def sendMessage(edge: EdgeTriplet[VertexId, Unit]):
        Iterator[(VertexId, Map[VertexId, Long])] =
    {
      Iterator((edge.dstId, Map(edge.srcAttr -> 1L)), (edge.srcId, Map(edge.dstAttr -> 1L)))
    }

    def messageCombiner(a: Map[VertexId, Long], b: Map[VertexId, Long]): Map[VertexId, Long] = {
      (a.keySet ++ b.keySet).map(label =>
        label -> (a.getOrElse(label, 0L) + b.getOrElse(label, 0L))
      ).toMap
    }

    val initialMessage = Map[VertexId, Long]()

    LineagePregel(
      cdlpGraph, initialMessage, activeDirection = EdgeDirection.Out
    )(
      vertexProgram, sendMessage, messageCombiner
    )
  }
}
