package lu.magalhaes.gilles.provxlib.lineage

import lu.magalhaes.gilles.provxlib.lineage.hooks.HooksRegistry
import lu.magalhaes.gilles.provxlib.lineage.modes.{InteractiveMode, Mode}
import lu.magalhaes.gilles.provxlib.lineage.storage.{DefaultStorageHandler, StorageHandler}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexId

class LineageLocalContext(@transient val sparkContext: SparkContext) {

  private var checkpointingEnabled = false

  val hooksRegistry = new HooksRegistry()

  private var storageHandler: StorageHandler = new DefaultStorageHandler(this)

  private var mode: Mode = new InteractiveMode()

  def getMode(): Mode = mode
  def setMode(m: Mode): Unit = mode = m

  var pruneLineage: Option[((VertexId, Any)) => Boolean] = None
  var sampleFraction: Option[Double] = None

  def getStorageHandler: StorageHandler = storageHandler

  def setStorageHandler(handler: StorageHandler): Unit = {
    storageHandler = handler
  }

  def enableTracing(): Unit = {
    require(storageHandler.getLineageDir.isDefined, "Lineage directory is not defined.")
    checkpointingEnabled = true
  }
  def disableTracing(): Unit = {
    checkpointingEnabled = false
  }

  def isTracingEnabled: Boolean = checkpointingEnabled
}
