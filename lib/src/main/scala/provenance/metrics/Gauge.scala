package lu.magalhaes.gilles.provxlib
package provenance.metrics

case class Gauge[T](name: String, value: T) extends Observation
