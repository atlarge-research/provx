package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.metrics.ObservationSet

trait PregelEventHook {
  def preStart(set: ObservationSet): Unit = {}

  def postStop(set: ObservationSet): Unit = {}

  def preIteration(set: ObservationSet): Unit = {}

  def postIteration(set: ObservationSet): Unit = {}
}
