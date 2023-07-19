package lu.magalhaes.gilles.provxlib
package lineage.metrics

case class Counter private (name: String, value: Long = 0) extends Observation(name = name) {

  def get: Long = value

  def increment(): Counter = new Counter(name, value + 1)

  def decrement(): Counter = new Counter(name, value - 1)

  def serialize(): ujson.Obj = super.serialize("Counter", value)
}

object Counter {
  def zero(name: String) : Counter = {
    new Counter(name, 0)
  }
}