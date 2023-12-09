package lu.magalhaes.gilles.provxlib
package provenance.metrics

import org.scalatest.funsuite.AnyFunSuite

class MetricsTestSuite extends AnyFunSuite {
  test("Observation") {
    var counter = Counter.zero("test")
    counter = counter.increment()
    counter = counter.increment()
    counter = counter.decrement()
    assert(counter.get == 1)
  }

  test("ObservationSet") {
    val obsSet = ObservationSet()
    obsSet.add(Gauge("hi", 30))
    obsSet.add(Counter.zero("test1").increment())

    val obsSet2 = ObservationSet()
    obsSet2.add(Gauge("welcome", 60))
    obsSet2.add(Counter.zero("test2").increment().increment())

    obsSet.add(obsSet2)

    println(JSONSerializer.serialize(obsSet))
  }
}
