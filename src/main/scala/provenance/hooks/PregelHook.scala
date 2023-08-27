package lu.magalhaes.gilles.provxlib
package provenance.hooks

import provenance.GraphLineage
import provenance.events.{EventType, PregelAlgorithm}

import scala.reflect.ClassTag

abstract class PregelHook(lifecycleHooks: Seq[Hook], iterationHooks: Seq[Hook])
    extends Hook {

  // TODO: this should only be invoked on pregel use
  override def shouldInvoke(event: EventType): Boolean = {
    event match {
      case PregelAlgorithm() => true
      case _                 => false
    }
  }

  def preStart[VD: ClassTag, ED: ClassTag](
      preprocessedGraph: GraphLineage[VD, ED]
  ): Unit =
    lifecycleHooks.foreach(_.pre(preprocessedGraph))

  def postStop[VD: ClassTag, ED: ClassTag](
      postprocessedGraph: GraphLineage[VD, ED]
  ): Unit =
    lifecycleHooks.foreach(_.post(postprocessedGraph))

  def preIteration[VD: ClassTag, ED: ClassTag](
      preIterationGraph: GraphLineage[VD, ED]
  ): Unit =
    iterationHooks.foreach(_.pre(preIterationGraph))

  def postIteration[VD: ClassTag, ED: ClassTag](
      postIterationGraph: GraphLineage[VD, ED]
  ): Unit =
    iterationHooks.foreach(_.post(postIterationGraph))
}
