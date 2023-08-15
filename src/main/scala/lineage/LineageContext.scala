package lu.magalhaes.gilles.provxlib
package lineage

import lineage.hooks.HooksRegistry
import lineage.storage.{NullStorageHandler, StorageHandler}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

object LineageContext {

  private val nextGLId = new AtomicInteger(0)

  val elements: ArrayBuffer[GraphLineage[_, _]] = ArrayBuffer.empty

  private[lineage] def newGLId(gl: GraphLineage[_, _]): Int = {
    elements += gl
    nextGLId.getAndIncrement()
  }

  val hooks = new HooksRegistry()

  val graph = new ProvenanceGraph()

  var storageHandler: StorageHandler = new NullStorageHandler()

  def setStorageHandler(s: StorageHandler): Unit = {
    storageHandler = s
  }

  private var tracingEnabled = true
  def isTracingEnabled: Boolean = tracingEnabled

  def enableTracing(): Unit = {
    tracingEnabled = true
  }

  def disableTracing(): Unit = {
    tracingEnabled = false
  }
}
