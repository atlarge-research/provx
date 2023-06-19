package lu.magalhaes.gilles.provxlib
package lineage

import lineage.metrics.ObservationSet

trait PregelLifecycle {
  def preStart(set: ObservationSet): Unit = {}

  def postStop(set: ObservationSet): Unit = {}

  def preIteration(set: ObservationSet): Unit = {}

  def postIteration(set: ObservationSet): Unit = {}
}
