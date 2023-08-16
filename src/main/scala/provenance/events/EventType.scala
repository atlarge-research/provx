package lu.magalhaes.gilles.provxlib
package provenance.events

trait EventType

case class Algorithm(name: String) extends EventType
case class Operation(name: String) extends EventType
case class PregelAlgorithm() extends EventType
