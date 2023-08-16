package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.{EventType, GraphLineage}
import lineage.metrics.TimeUnit

import scala.reflect.ClassTag

case class TimeHook(gaugeName: String) extends Hook {

  var startTime: Option[Long] = None

  // TODO: make this a parameter when this should be executed
  override def shouldInvoke(event: EventType): Boolean = true

  override def pre[VD: ClassTag, ED: ClassTag](
      inputGraph: GraphLineage[VD, ED]
  ): Unit = {
    assert(startTime.isEmpty, "start time already set")
    startTime = Some(System.nanoTime())
  }

  override def post[VD: ClassTag, ED: ClassTag](
      outputGraph: GraphLineage[VD, ED]
  ): Unit = {
    assert(startTime.isDefined, "pre method not called on TimeHook")

    val iterationTime = System.nanoTime() - startTime.get
    assert(iterationTime >= 0)
    outputGraph.metrics.add(TimeUnit(gaugeName, iterationTime, "ns"))
    startTime = None
  }
}
