package lu.magalhaes.gilles.provxlib
package provenance.query

import org.apache.spark.graphx.{EdgeTriplet, VertexId}

import scala.reflect.ClassTag

case class DataPredicate(
    nodePredicate: (VertexId, Any) => Boolean = (_: VertexId, _: Any) => true,
    edgePredicate: EdgeTriplet[_, _] => Boolean = (_: EdgeTriplet[_, _]) => true
)
