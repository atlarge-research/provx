package lu.magalhaes.gilles.provxlib
package provenance.query

import provenance.ProvenanceGraph

case class ProvenancePredicate(
    nodePredicate: ProvenanceGraph.NodePredicate,
    edgePredicate: ProvenanceGraph.EdgePredicate
)
