package lu.magalhaes.gilles.provxlib

import scalax.collection.immutable.Graph

package object provenance {
  type ProvenanceGraphType =
    Graph[ProvenanceGraph.Node, ProvenanceGraph.Relation]
}
