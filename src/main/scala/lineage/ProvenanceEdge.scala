package lu.magalhaes.gilles.provxlib
package lineage

import scalax.collection.generic.AbstractDiEdge

import scala.reflect.ClassTag

sealed trait PregelPhase
case class PreStart() extends PregelPhase
case class PreIteration() extends PregelPhase

case class PostIteration() extends PregelPhase

case class PostStop() extends PregelPhase

sealed trait EventType

case class Algorithm(name: String) extends EventType
case class Operation(name: String) extends EventType

sealed trait Pregel extends EventType

case class PregelLifecycle() extends Pregel

case class PregelIteration(iteration: Long) extends Pregel

//case class TransitiveSubsetExpr(superset: EventType, subset: EventType) extends EventType
//case class DirectSubsetExpr(superset: EventType, subset: EventType) extends EventType

//case class Expr(lhs: ProvenanceEdgeType) {
//  def \\(rhs: ProvenanceEdgeType): ProvenanceEdgeType = {
//    SubsetExpr(lhs, rhs)
//  }
//}

//object Helpers {
//  implicit class EdgeTypeWithCompose(lhs: EventType) {
//    def \\(rhs: EventType): EventType = {
//      TransitiveSubsetExpr(lhs, rhs)
//    }
//
//    def ->(rhs: EventType): EventType = {
//      DirectSubsetExpr(lhs, rhs)
//    }
//  }
//}
//Expr(Operation("mapVertices")) \\ Operation

//import Helpers._


// TODO: filter validator


//val expr = Algorithm("pageRank") -> PregelLifecycle -> PregelIteration(1)


sealed trait ProvenanceData
case class ProvenanceSource[VD: ClassTag, ED: ClassTag](
  inputGraph: GraphLineage[VD, ED]
) extends ProvenanceData

case class ProvenanceTriplet[VD: ClassTag, ED: ClassTag, VD2: ClassTag, ED2: ClassTag](
                                                                                        inputGraph: GraphLineage[VD, ED],
                                                                                        edge: EventType,
                                                                                        outputGraph: GraphLineage[VD2, ED2]
) extends ProvenanceData
