package lu.magalhaes.gilles.provxlib
package lineage

import lineage.ProvenanceGraph.{Node, Relation}

import scalax.collection.edges.{DiEdge, DiEdgeImplicits}
import scalax.collection.generic.{AbstractDiEdge, Edge}
import scalax.collection.immutable.Graph
import scalax.collection.io.dot.{DotAttr, DotEdgeStmt, DotGraph, DotNodeStmt, DotRootGraph, Graph2DotExport, Id, NodeId}
import scalax.collection.AnyGraph

object ProvenanceGraph {
  type NodePredicate = Node => Boolean
  type EdgePredicate = Relation => Boolean

  val noNode: NodePredicate = _ => false
  val noEdge: EdgePredicate = _ => false

  case class Node(g: GraphLineage[_, _])

  case class Relation(input: Node, output: Node, event: EventType)
    extends AbstractDiEdge(input, output)

  implicit class MyLDiEdgeInfixLabelConstructor(val e: DiEdge[Node]) extends AnyVal {
    def :+(label: EventType): Relation = Relation(e.source, e.target, label)
  }
}

class ProvenanceGraph(var graph: Graph[Node, Relation] = Graph.empty) {
  import ProvenanceGraph._

  def add(source: GraphLineage[_, _], target: GraphLineage[_, _], attr: EventType): Unit = {
    graph = graph + (Node(source) ~> Node(target) :+ attr)
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
          NodeId(s"G${innerEdge.outer.input.g.id}"),
          NodeId(s"G${innerEdge.outer.output.g.id}"),
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
      val g = innerNode.outer.g
      val nodeId = g.id
      Some(
        root, DotNodeStmt(
          NodeId(s"G${nodeId}"),
          attrList = List(
            DotAttr(Id("fillcolor"), Id("cadetblue1")),
            DotAttr(Id("style"), Id("filled")),
            DotAttr(Id("label"), Id(s"G${nodeId}\\n${g.storageLocation.getOrElse("")}")),
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

  def filter(nodeP: NodePredicate, edgeP: EdgePredicate): ProvenanceGraph = {
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

  def byId(id: Long): Option[GraphLineage[_, _]] = {
    graph.nodes.find((node: Graph[ProvenanceGraph.Node, ProvenanceGraph.Relation]#NodeT) => {
      node.outer.g.id == id
    }).map(_.outer.g)
  }

  def export(): String = {
    // TODO: implement this myself
    throw new NotImplementedError("no export")
  }

  def load(): String = {
    // TODO: implement this myself
    throw new NotImplementedError("no export")
  }
}
