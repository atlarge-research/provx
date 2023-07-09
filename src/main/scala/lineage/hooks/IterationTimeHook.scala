package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.metrics.ObservationSet

class IterationTimeHook extends PregelEventHook {

  val timeHook = new TimeHook("iterationTime")

  override def preIteration(set: ObservationSet): Unit = timeHook.pre()

  override def postIteration(set: ObservationSet): Unit = timeHook.post(set)
}
