package lu.magalhaes.gilles.provxlib
package lineage.metrics

abstract class Observation(name: String) {
  def serialize[T](observationType: String, value: ujson.Value): ujson.Obj = ujson.Obj(
    "type" -> observationType,
    "name" -> name,
    "value" -> value,
  )

  def serialize(): ujson.Obj

  override def toString: String = serialize().toString()
}
