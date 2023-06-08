package lu.magalhaes.gilles.provxlib
package lineage.metrics

import org.scalatest.funsuite.AnyFunSuite

class MetricsTestSuite extends AnyFunSuite {
  test("Observation") {
    var counter = Counter.zero
    counter = counter.increment()
    counter = counter.increment()
    assert(counter.current == 2)
  }

  test("ObservationSet") {
    val obsSet = ObservationSet()
    obsSet.add(Gauge("hi", 30))
    obsSet.add(Counter.zero.increment())

    val obsSet2 = ObservationSet()
    obsSet2.add(Gauge("welcome", 60))
    obsSet2.add(Counter.zero.increment().increment())

    obsSet.add(obsSet2)

    println(obsSet.serialize())
  }
}
