package lu.magalhaes.gilles.provxlib
package lineage

import lineage.hooks.HooksRegistry
import lineage.storage.{NullStorageHandler, StorageHandler}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer



object LineageContext {

  private val nextGLId = new AtomicInteger(0)

  private[lineage] def newGLId(gl: GraphLineage[_, _]): Int = {
    elements += gl
    nextGLId.getAndIncrement()
  }

  val elements: ArrayBuffer[GraphLineage[_, _]] = ArrayBuffer.empty

  val hooks = new HooksRegistry()
  val graph = new ProvenanceGraph()

  val tracingStatus = new Toggle()
  val storageStatus = new Toggle()

  def isTracingEnabled: Boolean = tracingStatus.isEnabled

  def isStorageEnabled: Boolean = storageStatus.isEnabled

  var storageHandler: StorageHandler = new NullStorageHandler()

  def setStorageHandler(s: StorageHandler): Unit = {
    storageHandler = s
  }
}
