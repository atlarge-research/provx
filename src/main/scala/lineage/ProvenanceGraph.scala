package lu.magalhaes.gilles.provxlib
package lineage

import lineage.ProvenanceGraph.{Node, Relation}

import scalax.collection.edges.{DiEdge, DiEdgeImplicits}
import scalax.collection.generic.{AbstractDiEdge, Edge}
import scalax.collection.immutable.Graph
import scalax.collection.io.dot.{DotAttr, DotEdgeStmt, DotGraph, DotNodeStmt, DotRootGraph, Graph2DotExport, Id, NodeId}
import scalax.collection.{AnyGraph, GraphLike}

object ProvenanceGraph {
  case class Node(g: Option[GraphLineage[_, _]])

  case class Relation(input: Node, output: Node, event: EventType)
    extends AbstractDiEdge(input, output)

  implicit class MyLDiEdgeInfixLabelConstructor(val e: DiEdge[Node]) extends AnyVal {
    def :+(label: EventType): Relation = Relation(e.source, e.target, label)
  }
}

class ProvenanceGraph(var graph: Graph[Node, Relation] = Graph.empty) {

  import ProvenanceGraph._

  def add(source: GraphLineage[_, _], target: GraphLineage[_, _], attr: EventType): Unit = {
    graph = graph + (Node(Some(source)) ~> Node(Some(target)) :+ attr)
  }

  def toDot(): String = {
    val root = DotRootGraph(
      directed = true,
      id = None,
    )
    def edgeTransformer(innerEdge: Graph[Node, Relation]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.outer.event.toString
      val edgeColor = innerEdge.outer.event match {
        case Algorithm(_) => "gold"
        case Operation(_) => "indianred2"
        case PregelAlgorithm() => "green"
        case _ => "black"
      }
      Some(
        root,
        DotEdgeStmt(
          NodeId(s"G${innerEdge.outer.input.g.get.id}"),
          NodeId(s"G${innerEdge.outer.output.g.get.id}"),
          List(
            DotAttr(Id("label"), Id(edge)),
            DotAttr(Id("color"), Id(edgeColor)),
            DotAttr(Id("fontcolor"), Id(edgeColor)),
            DotAttr(Id("style"), Id("filled")),
          )
        )
      )
    }

    def nodeTransformer(innerNode: Graph[Node, Relation]#NodeT): Option[(DotGraph, DotNodeStmt)] = {
      val nodeId = innerNode.outer.g.get.id
      Some(
        root, DotNodeStmt(
          NodeId(s"G${nodeId}"),
          attrList = List(
            DotAttr(Id("fillcolor"), Id("cadetblue1")),
            DotAttr(Id("style"), Id("filled")),
          )

        )
      )
    }



    graph.toDot(
      root,
      edgeTransformer = edgeTransformer,
      cNodeTransformer = Some(nodeTransformer)
    )
  }

  type NodePredicate = Node => Boolean
  type EdgePredicate = Relation => Boolean

  val anyNode: NodePredicate = _ => true
  val anyEdge: EdgePredicate = _ => true

  def filter(nodeP: NodePredicate = anyNode, edgeP: EdgePredicate = anyEdge): ProvenanceGraph = {
    // required for some typing business that I'm not smart enough to understand
    implicit class ForIntelliJ[N, E <: Edge[N]](val g: Graph[N, E]) {
      def asAnyGraph: AnyGraph[N, E] = g
    }

    val g = graph.asAnyGraph
    def nodePred(n: g.NodeT): Boolean = nodeP(n.outer)
    def edgePred(e: g.EdgeT): Boolean = edgeP(e.outer)

    val result = g filter (nodeP = nodePred, edgeP = edgePred)

    new ProvenanceGraph(result.asInstanceOf[Graph[Node, Relation]])
  }

  // TODO: fix next/previous, source and sink operations on provenance graph

//  def next(gl: GraphLineage[_, _]): Seq[GraphLineage[_, _]] = {
//  }
//
//  def previous(gl: GraphLineage[_, _]): Seq[GraphLineage[_, _]] = {
//  }

//  def first(gl: GraphLineage[_, _]): Seq[GraphLineage[_, _]] = {
//  }
//
//  def last(gl: GraphLineage[_, _]): Seq[GraphLineage[_, _]] = {
//  }
}
