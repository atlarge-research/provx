package lu.magalhaes.gilles.provxlib
package lineage.metrics

case class Counter private (name: String, value: Long = 0) extends Observation(name = name) {

  def current: Long = value

  def increment(): Counter = new Counter(name, value + 1)

  def decrement(): Counter = new Counter(name, value - 1)

  override def serialize(): ujson.Obj = {
    ujson.Obj(
      "type" -> "Counter",
      "name" -> name,
      "value" -> value,
    )
  }
}

object Counter {
  def zero(name: String) : Counter = {
    new Counter(name, 0)
  }
}