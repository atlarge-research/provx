package lu.magalhaes.gilles.provxlib
package provenance.events

trait PregelEvent extends EventType
case class PregelLifecycleStart() extends PregelEvent
case class PregelLifecycleStop() extends PregelEvent
case class PregelIteration(iteration: Long) extends PregelEvent
