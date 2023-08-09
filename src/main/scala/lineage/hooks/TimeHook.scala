package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.metrics.TimeUnit
import lineage.GraphLineage

import scala.reflect.ClassTag

case class TimeHook(gaugeName: String) extends Hook {

  var startTime: Option[Long] = None

  override def pre[VD: ClassTag, ED: ClassTag](inputGraph: GraphLineage[VD, ED]): Unit = {
    startTime = Some(System.nanoTime())
  }

  override def post[VD: ClassTag, ED: ClassTag](outputGraph: GraphLineage[VD, ED]): Unit = {
    assert(startTime.isDefined, "pre method not called on TimeHook")

    val iterationTime = System.nanoTime() - startTime.get
    assert(iterationTime >= 0)
    outputGraph.metrics.add(TimeUnit(gaugeName, iterationTime, "ns"))
    startTime = None
  }
}
