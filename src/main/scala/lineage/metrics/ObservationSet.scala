package lu.magalhaes.gilles.provxlib
package lineage.metrics

import scala.collection.mutable.ArrayBuffer

case class ObservationSet() extends Observation(name = "obs-set") {

  private val observations = ArrayBuffer[Observation]()

  def add(element: Observation): Unit = {
    observations += element
  }

  def values: Set[Observation] = observations.toSet

  def filter(f: Observation => Boolean): Set[Observation] = {
    observations.filter(f).toSet
  }

  def serialize(): ujson.Obj =
    ujson.Obj(
      "type" -> "ObservationSet",
      "values" -> ujson.Arr(observations.map {
        case c: Counter => c.serialize()
        case g: Gauge => g.serialize()
        case o: ObservationSet => o.serialize()
      })
    )

}
