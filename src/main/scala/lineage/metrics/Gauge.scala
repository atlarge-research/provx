package lu.magalhaes.gilles.provxlib
package lineage.metrics

case class Gauge(gaugeName: String, var value: Long) extends Observation(name = gaugeName) {

  def set(newValue: Long): Unit = {
    value = newValue
  }

  def get: Long = value

  override def serialize(): ujson.Obj = ujson.Obj(
    "type" -> "Gauge",
    "name" -> gaugeName,
    "value" -> value,
  )
}