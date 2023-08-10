package lu.magalhaes.gilles.provxlib
package lineage.metrics

case class Gauge[T](name: String, value: T) extends Observation