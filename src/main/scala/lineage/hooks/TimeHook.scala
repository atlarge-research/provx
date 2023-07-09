package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.metrics.ObservationSet
import lineage.metrics.Gauge

class TimeHook(val gaugeName: String) {
  var startTime: Option[Long] = None
  def pre(): Unit = {
    startTime = Some(System.nanoTime())
  }

  def post(set: ObservationSet): Unit = {
    val iterationTime = System.nanoTime() - startTime.get
    assert(iterationTime >= 0)
    set.add(Gauge(gaugeName, iterationTime))
    startTime = None
  }
}
