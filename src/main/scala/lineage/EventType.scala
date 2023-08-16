package lu.magalhaes.gilles.provxlib
package lineage

sealed trait EventType

case class Algorithm(name: String) extends EventType
case class Operation(name: String) extends EventType
case class PregelAlgorithm() extends EventType

trait PregelEvent extends EventType
case class PregelLifecycleStart() extends PregelEvent
case class PregelLifecycleStop() extends PregelEvent
case class PregelIteration(iteration: Long) extends PregelEvent
