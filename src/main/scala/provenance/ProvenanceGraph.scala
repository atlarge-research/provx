package lu.magalhaes.gilles.provxlib
package provenance

import provenance.events._
import provenance.metrics.{JSONSerializer, ObservationSet}

import scalax.collection.{AnyGraph, OneOrMore}
import scalax.collection.edges.{DiEdge, DiEdgeImplicits}
import scalax.collection.generic.{
  AbstractDiEdge,
  MultiEdge,
  Edge => GenericEdge
}
import scalax.collection.immutable.Graph
import scalax.collection.io.dot.{
  DotAttr,
  DotEdgeStmt,
  DotGraph,
  DotNodeStmt,
  DotRootGraph,
  Graph2DotExport,
  Id,
  NodeId
}

object ProvenanceGraph {
  type NodePredicate = Node => Boolean
  type EdgePredicate = Relation => Boolean

  val noNode: NodePredicate = _ => false
  val noEdge: EdgePredicate = _ => false

  val allNodes: NodePredicate = _ => true
  val allEdges: EdgePredicate = _ => true

  type Type = Graph[ProvenanceGraph.Node, ProvenanceGraph.Relation]

  case class Node(g: ProvenanceGraphNode)
  case class Edge(event: EventType, metrics: ObservationSet)

  case class Relation(input: Node, output: Node, edge: Edge)
      extends AbstractDiEdge(input, output)
      with MultiEdge {
    override def extendKeyBy: OneOrMore[Any] = OneOrMore.one(edge)
  }

  implicit class MyLDiEdgeInfixLabelConstructor(val e: DiEdge[Node])
      extends AnyVal {
    def :+(edge: Edge): Relation = Relation(e.source, e.target, edge)
  }
}

class ProvenanceGraph(var graph: ProvenanceGraph.Type = Graph.empty) {
  import ProvenanceGraph._

  def add(
      source: ProvenanceGraphNode,
      target: ProvenanceGraphNode,
      edge: Edge
  ): Unit = {
    graph = graph + (Node(source) ~> Node(target) :+ edge)
  }

  def toDot(): String = {
    val root = DotRootGraph(
      directed = true,
      id = None
    )
    def edgeTransformer(
        innerEdge: ProvenanceGraph.Type#EdgeT
    ): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.outer.edge.event.toString
      val edgeColor = innerEdge.outer.edge.event match {
        case PageRank(_, _) | BFS(_) | SSSP(_) | WCC(_) => "darkorange"
        case Operation(_)                               => "indianred2"
        case PregelAlgorithm()                          => "green"
        case PregelLifecycleStart()                     => "darkviolet"
        case PregelIteration(_)                         => "darkorchid1"
        case _                                          => "black"
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
    implicit class ForIntelliJ[N, E <: GenericEdge[N]](val g: Graph[N, E]) {
      def asAnyGraph: AnyGraph[N, E] = g
    }

    val g = graph.asAnyGraph
    def nodePred(n: g.NodeT): Boolean = nodeP(n.outer)
    def edgePred(e: g.EdgeT): Boolean = edgeP(e.outer)

    val result = g filter (nodeP = nodePred, edgeP = edgePred)

    new ProvenanceGraph(result.asInstanceOf[Graph[Node, Relation]])
  }

  def toJson(): ujson.Obj = {
    val nodes = graph.edges
      .flatMap((e: ProvenanceGraph.Type#EdgeT) =>
        Seq(e.outer.input.g, e.outer.output.g)
      )
      .map((g: ProvenanceGraphNode) =>
        ujson.Obj(
          "id" -> g.id,
          "location" -> g.storageLocation.getOrElse("").toString
        )
      )

    val edges = graph.edges.map((e: ProvenanceGraph.Type#EdgeT) => {
      val eventType = e.outer.edge.event match {
        case _: Algorithm           => "algorithm"
        case Operation(_)           => "operation"
        case PregelAlgorithm()      => "pregel"
        case PregelLifecycleStart() => "pregelStart"
        case PregelLifecycleStop()  => "pregelStop"
        case PregelIteration(_)     => "pregelIteration"
        case _                      => "<unknown>"
      }
      ujson.Obj(
        "source" -> e.outer.input.g.id,
        "target" -> e.outer.output.g.id,
        "relationship" -> e.outer.edge.event.toString,
        "type" -> eventType,
        "metrics" -> JSONSerializer.serialize(e.outer.edge.metrics)
      )
    })
    ujson
      .Obj(
        "nodes" -> nodes,
        "edges" -> edges
      )
  }

  def byId(id: Long): Option[ProvenanceGraphNode] = {
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
