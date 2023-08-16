package lu.magalhaes.gilles.provxlib
package lineage.metrics

case class Counter private (name: String, value: Long = 0) extends Observation {

  def get: Long = value

  def increment(): Counter = Counter(name, value + 1)

  def decrement(): Counter = Counter(name, value - 1)
}

object Counter {
  def zero(name: String): Counter = {
    new Counter(name, 0)
  }
}
