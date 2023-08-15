package lu.magalhaes.gilles.provxlib
package lineage.hooks

import lineage.{EventType, GraphLineage}

import scala.reflect.ClassTag

abstract class Hook {

  def shouldInvoke(event: EventType): Boolean = true

  // no way to pass in metadata to pre and post methods

  def pre[VD: ClassTag, ED: ClassTag](inputGraph: GraphLineage[VD, ED]): Unit = {}

  def post[VD: ClassTag, ED: ClassTag](outputGraph: GraphLineage[VD, ED]): Unit = {}
}
