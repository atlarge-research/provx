package lu.magalhaes.gilles.provxlib
package lineage.metrics

abstract class Observation(name: String) {
  def serialize(): ujson.Obj
}
