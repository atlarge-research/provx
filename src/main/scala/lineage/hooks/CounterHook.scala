package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.metrics.Counter
import lineage.GraphLineage

import scala.reflect.ClassTag

case class CounterHook() extends Hook {
  private var counter = Counter.zero("iteration")

  override def post[VD: ClassTag, ED: ClassTag](
      inputGraph: GraphLineage[VD, ED]
  ): Unit = {
    inputGraph.metrics.add(counter)
    counter = counter.increment()
  }

  // TODO: how do we reset this when running multiple algorithms?
}
