package lu.magalhaes.gilles.provxlib
package lineage

import scala.collection.mutable.ArrayBuffer

class ProvenanceGraph {

  private type GraphLineagePair = (GraphLineage[_, _], GraphLineage[_, _])
  private def GraphLineagePair(
    inputGraph: GraphLineage[_, _],
    outputGraph: GraphLineage[_, _]) = (inputGraph, outputGraph)

  val pairs: ArrayBuffer[GraphLineagePair] = ArrayBuffer.empty

  val elements: scala.collection.mutable.Map[Long, GraphLineage[_, _]] = scala.collection.mutable.Map.empty

  def chain(inputGraph: GraphLineage[_, _], outputGraph: GraphLineage[_, _]): Unit = {
    pairs += GraphLineagePair(inputGraph, outputGraph)
    if (elements.contains(inputGraph.id)) {
      assert(elements(inputGraph.id) eq inputGraph, "Reassigning input graph not allowed")
    }

    if (elements.contains(outputGraph.id)) {
      assert(elements(outputGraph.id) eq outputGraph, "Reassigning output graph not allowed")
    }

    elements(inputGraph.id) = inputGraph
    elements(outputGraph.id) = outputGraph
  }

  def toDot(withEdges: Boolean = true, withVertices: Boolean = true): String = {
    val sb = new StringBuilder()
    sb.append("digraph {\n")

    for (element <- elements.keys.toSeq.sorted) {
      val g = elements(element)
      val annotations = g.annotations.mkString(",")
      sb.append(s"""G${element} [fillcolor=cadetblue1 style=filled, label="G${element}-${annotations}"];\n""")
      if (withEdges) {
        sb.append(s"""E${g.edges.id} [fillcolor=gold style=filled, label="E${g.edges.id}"];\n""")
        sb.append(s"""E${g.edges.id} -> G${element};\n""")
      }
      if (withVertices) {
        sb.append(s"""V${g.vertices.id} [fillcolor=indianred2 style=filled, label="V${g.vertices.id}"];\n""")
        sb.append(s"""V${g.vertices.id} -> G${element};\n""")
      }
    }

    for (pair <- pairs) {
      sb.append(s"""G${pair._1.id} -> G${pair._2.id} [color="orchid"];\n""")
    }
    sb.append("}\n")
    sb.toString()
  }

  def next(gl: GraphLineage[_, _]): Seq[GraphLineage[_, _]] = {
    pairs.filter(x => x._1.id == gl.id).map(g => g._2)
  }

}
