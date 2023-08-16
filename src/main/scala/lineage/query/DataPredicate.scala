package lu.magalhaes.gilles.provxlib
package lineage.query

import org.apache.spark.graphx.{EdgeTriplet, VertexId}

import scala.reflect.ClassTag

case class DataPredicate[VD: ClassTag, ED: ClassTag](
    nodePredicate: (VertexId, VD) => Boolean = (_: VertexId, _: VD) => true,
    edgePredicate: EdgeTriplet[VD, ED] => Boolean = (_: EdgeTriplet[VD, ED]) =>
      true
)
