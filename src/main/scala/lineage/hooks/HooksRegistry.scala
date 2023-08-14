package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.{GraphLineage, EventType, ProvenanceGraph}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

class HooksRegistry {

  private val hooks: ArrayBuffer[Hook] = ArrayBuffer(
    DefaultPregelHook()
  )

  def register[PreD: ClassTag, PostD: ClassTag](hook: Hook): Unit = {
    hooks += hook
  }

  def deregister[PreD: ClassTag, PostD: ClassTag](hook: Hook): Unit = {
    val newHooks = hooks.filter(_ ne hook)
    hooks.clear()
    hooks ++= newHooks
  }

  def all: Iterable[Hook] = hooks

  def handlePre[VD: ClassTag, ED: ClassTag](event: ProvenanceGraph.Relation, g: GraphLineage[VD, ED]): Unit = {
    all.filter(_.shouldInvoke(event)).foreach(_.pre(g))
  }

  def handlePost[VD: ClassTag, ED: ClassTag](event: ProvenanceGraph.Relation, g: GraphLineage[VD, ED]): Unit = {
    all.filter(_.shouldInvoke(event)).foreach(_.post(g))
  }

  def clear(): Unit = {
    hooks.clear()
  }
}
