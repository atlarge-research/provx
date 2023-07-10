package lu.magalhaes.gilles.provxlib
package lineage

import scala.collection.mutable.ArrayBuffer

class ProvenanceGraph {

  private type GraphLineagePair = (GraphLineage[Any, Any], GraphLineage[Any, Any])
  private def GraphLineagePair(
    inputGraph: GraphLineage[Any, Any],
    outputGraph: GraphLineage[Any, Any]) = (inputGraph, outputGraph)

  private val pairs: ArrayBuffer[GraphLineagePair] = ArrayBuffer.empty

  def allPairs: ArrayBuffer[GraphLineagePair] = pairs

  def chain(inputGraph: GraphLineage[Any, Any], outputGraph: GraphLineage[Any, Any]): Unit = {
    pairs += GraphLineagePair(inputGraph, outputGraph)
  }

}
