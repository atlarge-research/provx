package lu.magalhaes.gilles.provxlib
package lineage.metrics

import scala.collection.mutable.ArrayBuffer

case class ObservationSet(value: ArrayBuffer[Observation] = ArrayBuffer.empty)
    extends Observation {

  def add(element: Observation): Unit = {
    value += element
  }

  def values: Set[Observation] = value.toSet

  def merge(newObservation: ObservationSet): Unit = {
    value ++= newObservation.values
  }

  def filter(f: Observation => Boolean): Set[Observation] = {
    value.filter(f).toSet
  }
}
