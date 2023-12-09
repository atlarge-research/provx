package lu.magalhaes.gilles.provxlib
package provenance.hooks

import provenance.GraphLineage
import provenance.events.EventType

import scala.reflect.ClassTag

abstract class Hook {

  def shouldInvoke(event: EventType): Boolean = true

  // no way to pass in metadata to pre and post methods

  def pre[VD: ClassTag, ED: ClassTag](
      inputGraph: GraphLineage[VD, ED]
  ): Unit = {}

  def post[VD: ClassTag, ED: ClassTag](
      outputGraph: GraphLineage[VD, ED]
  ): Unit = {}
}
