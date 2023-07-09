package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.metrics.{Counter, ObservationSet}

class IterationCounterHook extends PregelEventHook {
  private var counter = Counter.zero("iteration")

  override def postIteration(set: ObservationSet): Unit = {
    counter = counter.increment()
    set.add(counter)
  }
}
