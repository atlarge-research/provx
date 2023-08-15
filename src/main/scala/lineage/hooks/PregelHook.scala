package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.{Algorithm, EventType, GraphLineage, Operation, ProvenanceGraph}

import scala.reflect.ClassTag

abstract class PregelHook(lifecycleHooks: Seq[Hook], iterationHooks: Seq[Hook]) extends Hook {

  // TODO: this should only be invoked on pregel use
  override def shouldInvoke(event: EventType): Boolean = {
    event match {
      case Algorithm(name) => name == "pregel"
      case _ => false
    }
  }

  def preStart[VD: ClassTag, ED: ClassTag](preprocessedGraph: GraphLineage[VD, ED]): Unit =
    lifecycleHooks.foreach(_.pre(preprocessedGraph))

  def postStop[VD: ClassTag, ED: ClassTag](postprocessedGraph: GraphLineage[VD, ED]): Unit =
    lifecycleHooks.foreach(_.post(postprocessedGraph))

  def preIteration[VD: ClassTag, ED: ClassTag](preIterationGraph: GraphLineage[VD, ED]): Unit =
    iterationHooks.foreach(_.pre(preIterationGraph))

  def postIteration[VD: ClassTag, ED: ClassTag](postIterationGraph: GraphLineage[VD, ED]): Unit =
    iterationHooks.foreach(_.post(postIterationGraph))
}
