package lu.magalhaes.gilles.provxlib.lineage

import lu.magalhaes.gilles.provxlib.lineage.hooks.HooksRegistry
import lu.magalhaes.gilles.provxlib.lineage.modes.{InteractiveMode, Mode}
import lu.magalhaes.gilles.provxlib.lineage.storage.{DefaultStorageHandler, StorageHandler}
import org.apache.spark.SparkContext

class LineageLocalContext(@transient val sparkContext: SparkContext) {

  private var tracingEnabled: Option[Boolean] = None

  val hooksRegistry = new HooksRegistry()

  private var storageHandler: StorageHandler = new DefaultStorageHandler(this)

  private var mode: Mode = InteractiveMode()

  def getMode(): Mode = mode
  def setMode(m: Mode): Unit = mode = m

  def getStorageHandler: StorageHandler = storageHandler

  def setStorageHandler(handler: StorageHandler): Unit = {
    storageHandler = handler
  }

  def enableTracing(): Unit = {
    require(storageHandler.getLineageDir.isDefined, "Lineage directory is not defined.")
    tracingEnabled = Some(true)
  }
  def disableTracing(): Unit = {
    tracingEnabled = Some(false)
  }

  // TODO: does tracing from LineageContext override this decision?
  def isTracingEnabled: Boolean = {
    if (tracingEnabled.isDefined) {
      tracingEnabled.get
    } else {
      LineageContext.isTracingEnabled
    }
  }
}
