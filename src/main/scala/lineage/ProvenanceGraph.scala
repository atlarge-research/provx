package lu.magalhaes.gilles.provxlib
package lineage

import scala.collection.mutable.ArrayBuffer

class ProvenanceGraph {

  private type GraphLineagePair = (GraphLineage[_, _], GraphLineage[_, _])
  private def GraphLineagePair(
    inputGraph: GraphLineage[_, _],
    outputGraph: GraphLineage[_, _]) = (inputGraph, outputGraph)

  private val pairs: ArrayBuffer[GraphLineagePair] = ArrayBuffer.empty

  def allPairs: ArrayBuffer[GraphLineagePair] = pairs

  def chain(inputGraph: GraphLineage[_, _], outputGraph: GraphLineage[_, _]): Unit = {
    pairs += GraphLineagePair(inputGraph, outputGraph)
  }

}
