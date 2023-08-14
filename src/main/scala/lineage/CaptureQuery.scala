package lu.magalhaes.gilles.provxlib
package lineage

import org.apache.spark.graphx.{EdgeTriplet, Graph, VertexId}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class CaptureQuery {

//  type EdgePredicate[VD, ED] = EdgeTriplet[VD, ED] => Boolean
//  type VertexPredicate[VD] = (VertexId, VD) => Boolean
//
//  private val operations: ArrayBuffer[(VertexPredicate[_], EdgePredicate[_, _])] = ArrayBuffer.empty
//
//  // What data? node/edges/vertices
//  // What operations? and how to select them
//
//  def node[VD: ClassTag](vpred: VertexPredicate[VD]): CaptureQuery = {
//    subgraph(vpred = vpred)
//  }
//
//  def edge[VD: ClassTag, ED: ClassTag](epred: EdgePredicate[VD, ED]): CaptureQuery = {
//    subgraph(epred = epred)
//  }
//
//  def subgraph[VD: ClassTag, ED: ClassTag](
//      epred: EdgePredicate[VD, ED] = _ => true,
//      vpred: VertexPredicate[VD] = (_, _) => true)
//    : CaptureQuery = {
//    val op: (VertexPredicate[VD], EdgePredicate[VD, ED]) = (vpred, epred)
//    operations += op
//    this
//  }
//
//  def apply[VD: ClassTag, ED: ClassTag, VD1: ClassTag, ED1: ClassTag](g: Graph[VD, ED]): Graph[VD1, ED1] = {
//    operations.foldLeft(g)((previous, f) => {
//      previous.subgraph(epred = f._2, vpred = f._1)
//    })
//  }
}
