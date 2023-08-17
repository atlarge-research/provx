package lu.magalhaes.gilles.provxlib
package provenance

import provenance.events._

import scalax.collection.{AnyGraph, OneOrMore}
import scalax.collection.edges.{DiEdge, DiEdgeImplicits}
import scalax.collection.generic.{AbstractDiEdge, Edge, MultiEdge}
import scalax.collection.immutable.Graph
import scalax.collection.io.dot.{DotAttr, DotEdgeStmt, DotGraph, DotNodeStmt, DotRootGraph, Graph2DotExport, Id, NodeId}

object ProvenanceGraph {
  type NodePredicate = Node => Boolean
  type EdgePredicate = Relation => Boolean

  val noNode: NodePredicate = _ => false
  val noEdge: EdgePredicate = _ => false

  val allNodes: NodePredicate = _ => true
  val allEdges: EdgePredicate = _ => true

  type Type = Graph[ProvenanceGraph.Node, ProvenanceGraph.Relation]

  case class Node(g: GraphLineage[_, _])

  case class Relation(input: Node, output: Node, event: EventType)
      extends AbstractDiEdge(input, output)
      with MultiEdge {
    override def extendKeyBy: OneOrMore[Any] = OneOrMore.one(event)
  }

  implicit class MyLDiEdgeInfixLabelConstructor(val e: DiEdge[Node])
      extends AnyVal {
    def :+(label: EventType): Relation = Relation(e.source, e.target, label)
  }
}

class ProvenanceGraph(var graph: ProvenanceGraph.Type = Graph.empty) {
  import ProvenanceGraph._

  def add(
      source: GraphLineage[_, _],
      target: GraphLineage[_, _],
      attr: EventType
  ): Unit = {
    graph = graph + (Node(source) ~> Node(target) :+ attr)
  }

  def toDot(): String = {
    val root = DotRootGraph(
      directed = true,
      id = None
    )
    def edgeTransformer(
        innerEdge: ProvenanceGraph.Type#EdgeT
    ): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.outer.event.toString
      val edgeColor = innerEdge.outer.event match {
        case Algorithm(_)           => "darkorange"
        case Operation(_)           => "indianred2"
        case PregelAlgorithm()      => "green"
        case PregelLifecycleStart() => "darkviolet"
        case PregelIteration(_)     => "darkorchid1"
        case _                      => "black"
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
            DotAttr(Id("style"), Id("filled"))
          )
        )
      )
    }

    def nodeTransformer(
        innerNode: ProvenanceGraph.Type#NodeT
    ): Option[(DotGraph, DotNodeStmt)] = {
      val g = innerNode.outer.g
      val nodeId = g.id
      Some(
        root,
        DotNodeStmt(
          NodeId(s"G${nodeId}"),
          attrList = List(
            DotAttr(Id("fillcolor"), Id("cadetblue1")),
            DotAttr(Id("style"), Id("filled")),
            DotAttr(
              Id("label"),
              Id(s"G${nodeId}\\n${g.storageLocation.getOrElse("")}")
            )
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

  def toJson(): String = {
    val nodes = graph.edges
      .flatMap((e: ProvenanceGraph.Type#EdgeT) =>
        Seq(e.outer.input.g, e.outer.output.g)
      )
      .map((g: GraphLineage[_, _]) =>
        ujson.Obj(
          "id" -> g.id,
          "location" -> g.storageLocation.getOrElse("").toString
        )
      )
    val edges = graph.edges.map((e: ProvenanceGraph.Type#EdgeT) =>
      ujson.Obj(
        "source" -> e.outer.input.g.id,
        "target" -> e.outer.output.g.id,
        "relationship" -> e.outer.event.toString
      )
    )
    ujson
      .Obj(
        "nodes" -> ujson.Arr(nodes),
        "edges" -> ujson.Arr(edges)
      )
      .toString()
  }

  def byId(id: Long): Option[GraphLineage[_, _]] = {
    graph.nodes
      .find((node: ProvenanceGraph.Type#NodeT) => {
        node.outer.g.id == id
      })
      .map(_.outer.g)
  }

  def load(): String = {
    // TODO: implement this myself
    throw new NotImplementedError("no import")
  }
}
