package lu.magalhaes.gilles.provxlib
package provenance.algorithms

import provenance.{GraphLineage, LineagePregel}

import org.apache.spark.graphx.{EdgeDirection, EdgeTriplet, VertexId}

import scala.reflect.ClassTag

object LineageSSSP {

  def run[VD: ClassTag, ED: ClassTag](
      gl: GraphLineage[VD, ED],
      source: VertexId
  ): GraphLineage[Double, Double] = {

    val ssspGraph = gl
      .mapVertices((vid, _) => {
        if (vid == source) {
          0.0
        } else {
          Double.PositiveInfinity
        }
      })
      .mapEdges(x => x.attr.toString.toDouble)
      .cache()

    def vertexProgram(
        id: VertexId,
        oldValue: Double,
        message: Double
    ): Double = {
      math.min(oldValue, message)
    }

    def sendMessage(
        edgeData: EdgeTriplet[Double, Double]
    ): Iterator[(VertexId, Double)] = {
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
      ssspGraph,
      initialMessage,
      activeDirection = EdgeDirection.Out
    )(
      vertexProgram,
      sendMessage,
      messageCombiner
    )
  }
}
