package lu.magalhaes.gilles.provxlib
package lineage

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

object LineageContext {

  // TODO(gm): investigate whether it's useful to have a global lineage context

  private val nextGLId = new AtomicInteger(0)

  val pairs: ArrayBuffer[GraphLineage[_, _]] = ArrayBuffer.empty

  private[lineage] def newGLId(gl: GraphLineage[_, _]): Int = {
    pairs += gl
    println("Incrementing GLId")
    nextGLId.getAndIncrement()
  }

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
