package lu.magalhaes.gilles.provxlib
package lineage.query

import lineage.ProvenanceGraph

case class ProvenancePredicate(
    nodePredicate: ProvenanceGraph.NodePredicate,
    edgePredicate: ProvenanceGraph.EdgePredicate
)
