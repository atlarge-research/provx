package lu.magalhaes.gilles.provxlib
package lineage

import java.util.concurrent.atomic.AtomicInteger

object LineageContext {

  // TODO(gm): investigate whether it's useful to have a global lineage context

  private val nextGLId = new AtomicInteger(0)

  private[lineage] def newGLId(): Int = nextGLId.getAndIncrement()

  val graph = new ProvenanceGraph()

  private var tracingEnabled = false
  def isTracingEnabled: Boolean = tracingEnabled

  def enableTracing(): Unit = {
    tracingEnabled = true
  }

  def disableTracing(): Unit = {
    tracingEnabled = false
  }
}
