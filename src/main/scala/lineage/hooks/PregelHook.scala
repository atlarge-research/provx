package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.GraphLineage

import scala.reflect.ClassTag

abstract class PregelHook(lifecycleHooks: Seq[Hook], iterationHooks: Seq[Hook]) extends Hook {
  def preStart[VD: ClassTag, ED: ClassTag](preprocessedGraph: GraphLineage[VD, ED]): Unit =
    lifecycleHooks.foreach(_.pre(preprocessedGraph))

  def postStop[VD: ClassTag, ED: ClassTag](postprocessedGraph: GraphLineage[VD, ED]): Unit =
    lifecycleHooks.foreach(_.post(postprocessedGraph))

  def preIteration[VD: ClassTag, ED: ClassTag](preIterationGraph: GraphLineage[VD, ED]): Unit =
    iterationHooks.foreach(_.pre(preIterationGraph))

  def postIteration[VD: ClassTag, ED: ClassTag](postIterationGraph: GraphLineage[VD, ED]): Unit =
    iterationHooks.foreach(_.post(postIterationGraph))
}
