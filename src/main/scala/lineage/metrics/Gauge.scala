package lu.magalhaes.gilles.provxlib
package lineage.metrics

case class Gauge(gaugeName: String, var value: ujson.Value) extends Observation(name = gaugeName) {

  def set(newValue: ujson.Value): Unit = {
    value = newValue
  }

  def get: ujson.Value = value

  def serialize(): ujson.Obj = super.serialize("Gauge", value)
}