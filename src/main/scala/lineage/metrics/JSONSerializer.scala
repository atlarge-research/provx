package lu.magalhaes.gilles.provxlib
package lineage.metrics

object JSONSerializer {
  def serialize(o: Observation): ujson.Obj = {
    o match {
      case c: Counter =>
        ujson.Obj(
          "type" -> c.getClass.getName,
          "name" -> c.name,
          "value" -> c.value
        )
      case g: Gauge[_] =>
        ujson.Obj(
          "type" -> g.getClass.getName,
          "name" -> g.name,
          "value" -> g.value.toString
        )
      case t: TimeUnit =>
        ujson.Obj(
          "type" -> "time",
          "name" -> t.name,
          "value" -> ujson.Obj(
            "amount" -> t.amount,
            "unit" -> t.unit
          )
        )
      case o: ObservationSet =>
        ujson.Obj(
          "type" -> "ObservationSet",
          "values" -> ujson.Arr(o.value.map(i => serialize(i)))
        )
    }
  }
}
