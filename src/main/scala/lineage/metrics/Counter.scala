package lu.magalhaes.gilles.provxlib
package lineage.metrics

case class Counter private (value: Long = 0) extends Observation(name = "Counter") {

  def current: Long = value

  def increment(): Counter = new Counter(value + 1)

  def decrement(): Counter = new Counter(value - 1)

  override def serialize(): ujson.Obj = {
    ujson.Obj(
      "type" -> "Counter",
      "value" -> value,
    )
  }
}

object Counter {
  def zero: Counter = {
    new Counter(0)
  }
}