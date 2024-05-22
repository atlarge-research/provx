package lu.magalhaes.gilles.provxlib
package provenance.query

import org.apache.spark.graphx.{EdgeTriplet, VertexId}
import org.apache.spark.rdd.RDD

trait DataPredicate

case class GraphPredicate(
    nodePredicate: (VertexId, Any) => Boolean = (_: VertexId, _: Any) => true,
    edgePredicate: EdgeTriplet[_, _] => Boolean = (_: EdgeTriplet[_, _]) => true
) extends DataPredicate

case class DeltaPredicate(
    sample: RDD[(VertexId, Any)]
) extends DataPredicate
