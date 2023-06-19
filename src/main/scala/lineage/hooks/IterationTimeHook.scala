package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.PregelLifecycle
import lineage.metrics.{Gauge, ObservationSet}

class IterationTimeHook extends PregelLifecycle {

  var startTime: Option[Long] = None

  override def preIteration(set: ObservationSet): Unit = {
    startTime = Some(System.nanoTime())
  }

  override def postIteration(set: ObservationSet): Unit = {
    val iterationTime = System.nanoTime() - startTime.get
    assert(iterationTime >= 0)
    set.add(Gauge("iterationTime", iterationTime))
    startTime = None
  }
}
