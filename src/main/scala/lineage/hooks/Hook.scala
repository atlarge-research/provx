package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.GraphLineage

import scala.reflect.ClassTag

abstract class Hook {

  def shouldInvoke(eventName: String): Boolean = true

  def pre[VD: ClassTag, ED: ClassTag](inputGraph: GraphLineage[VD, ED]): Unit = {}

  def post[VD: ClassTag, ED: ClassTag](outputGraph: GraphLineage[VD, ED]): Unit = {}
}
