package lu.magalhaes.gilles.provxlib
package provenance

import provenance.events.EventType

class ProvenanceQuery(g: GraphLineage[_, _]) {

  def granularity(filter: EventType => Boolean): ProvenanceQuery = {
    this
  }
  def node(): ProvenanceQuery = this

  def edge(): ProvenanceQuery = this

  def subgraph(): ProvenanceQuery = this

//  def next(): Seq[GraphLineage[_, _]] = {
//    LineageContext.graph.pairs.filter(x => x._1.id == g.id).map(_._2)
//  }
//
//  def previous(): Seq[GraphLineage[_, _]] = {
//    LineageContext.graph.pairs.filter(x => x._2.id == g.id).map(_._1)
//  }

//  def next(): GraphQuery = this
//
//  def previous(): GraphQuery = this

  def source(): ProvenanceQuery = this

  def sink(): ProvenanceQuery = this

}
