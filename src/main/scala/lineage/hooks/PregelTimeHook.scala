package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.PregelLifecycle
import lineage.metrics.{Gauge, ObservationSet}
class PregelTimeHook extends PregelLifecycle {

  var startTime: Option[Long] = None

  override def preStart(set: ObservationSet): Unit = {
    startTime = Some(System.nanoTime())
  }

  override def postStop(set: ObservationSet): Unit = {
    val iterationTime = System.nanoTime() - startTime.get
    assert(iterationTime >= 0)
    set.add(Gauge("pregelTime", iterationTime))
    startTime = None
  }
}
