package lu.magalhaes.gilles.provxlib
package lineage

import scalax.collection.edges.DiEdge
import scalax.collection.generic.AbstractDiEdge
import scalax.collection.immutable.Graph
import scalax.collection.edges.DiEdgeImplicits
import scalax.collection.io.dot.{DotAttr, DotEdgeStmt, DotGraph, DotNodeStmt, DotRootGraph, EdgeTransformer, Graph2DotExport, Id, NodeId}

object ProvenanceGraph {
  case class Node(g: Option[GraphLineage[_, _]])

  case class Relation(input: Node, output: Node, event: EventType)
    extends AbstractDiEdge(input, output)

  implicit class MyLDiEdgeInfixLabelConstructor(val e: DiEdge[Node]) extends AnyVal {
    def :+(label: EventType): Relation = Relation(e.source, e.target, label)
  }
}

class ProvenanceGraph {
  import ProvenanceGraph._

  var g: Graph[Node, Relation] = Graph.empty

  def add(source: GraphLineage[_, _], target: GraphLineage[_, _], attr: EventType): Unit = {
    g = g + (Node(Some(source)) ~> Node(Some(target)) :+ attr)
  }

  def toDot(): String = {
    val root = DotRootGraph(
      directed = true,
      id = None,
    )
    def edgeTransformer(innerEdge: Graph[Node, Relation]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.outer.event.toString
      Some(
        root,
        DotEdgeStmt(
          NodeId(innerEdge.outer.input.g.get.id),
          NodeId(innerEdge.outer.output.g.get.id),
          List(DotAttr(Id("label"), Id(edge)))
        )
      )
    }

    def nodeTransformer(innerNode: Graph[Node, Relation]#NodeT): Option[(DotGraph, DotNodeStmt)] = {
      val nodeId = innerNode.outer.g.get.id
      Some(
        root, DotNodeStmt(NodeId(nodeId))
      )
    }
    g.toDot(
      root,
      edgeTransformer = edgeTransformer,
      cNodeTransformer = Some(nodeTransformer)
    )
  }

//  def toDot(withDataEdges: Boolean = true, withDataVertices: Boolean = true): String = {
//    val sb = new StringBuilder()
//    sb.append("digraph {\n")
//
//    for (element <- elements.keys.toSeq.sorted) {
//      val g = elements(element)
//      val annotations = g.annotations.mkString(",")
//      sb.append(s"""G${element} [fillcolor=cadetblue1 style=filled, label="G${element}-${annotations}"];\n""")
//      if (withDataEdges) {
//        sb.append(s"""E${g.edges.id} [fillcolor=gold style=filled, label="E${g.edges.id}"];\n""")
//        sb.append(s"""E${g.edges.id} -> G${element};\n""")
//      }
//      if (withDataVertices) {
//        sb.append(s"""V${g.vertices.id} [fillcolor=indianred2 style=filled, label="V${g.vertices.id}"];\n""")
//        sb.append(s"""V${g.vertices.id} -> G${element};\n""")
//      }
//    }
//
//    for (triplet <- triplets) {
//      val line = triplet match {
//        case ProvenanceTriplet(inputGraph, _, outputGraph) =>
//          s"""G${inputGraph.id} -> G${outputGraph.id} [color="orchid"];\n"""
//        case _ => ""
//      }
//
//      sb.append(line)
//    }
//    sb.append("}\n")
//    sb.toString()
//  }

  // TODO: fix next/previous, source and sink operations on provenance graph

//  def next(gl: GraphLineage[_, _]): Seq[GraphLineage[_, _]] = {
//    pairs.filter(x => x._1.id == gl.id).map(g => g._2)
//  }
//
//  def previous(gl: GraphLineage[_, _]): Seq[GraphLineage[_, _]] = {
//    pairs.filter(x => x._2.id == gl.id).map(g => g._1)
//  }

//  def first(gl: GraphLineage[_, _]): Seq[GraphLineage[_, _]] = {
//    // traverse list of pairs up to root
//    // TODO: implement
//    throw new NotImplementedError("first not implemented yet")
//  }
//
//  def last(gl: GraphLineage[_, _]): Seq[GraphLineage[_, _]] = {
//    // traverse list of pairs
//    // TODO: implement
//    throw new NotImplementedError("last not implemented yet")
//  }
}
