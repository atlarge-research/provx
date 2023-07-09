package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.metrics.ObservationSet

class PregelTimeHook extends PregelEventHook {

  val timeHook = new TimeHook("pregelTime")

  override def preStart(set: ObservationSet): Unit = timeHook.pre()
  override def postStop(set: ObservationSet): Unit = timeHook.post(set)
}
