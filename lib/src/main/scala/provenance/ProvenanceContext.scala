package lu.magalhaes.gilles.provxlib
package provenance

import provenance.hooks.HooksRegistry
import provenance.storage.{NullStorageHandler, StorageHandler}

import lu.magalhaes.gilles.provxlib.provenance.events.PregelIteration
import lu.magalhaes.gilles.provxlib.provenance.query.CaptureFilter
import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

object ProvenanceContext {

  private val nextGLId = new AtomicInteger(0)

  var sparkContext: Option[SparkSession] = None

  private[provenance] def newGLId(gl: GraphLineage[_, _]): Int = {
    elements += gl
    nextGLId.getAndIncrement()
  }

  val elements: ArrayBuffer[GraphLineage[_, _]] = ArrayBuffer.empty

  val hooks = new HooksRegistry()
  val graph = new ProvenanceGraph()

  var captureFilter: Option[CaptureFilter] = None

  def setCaptureFilter(c: CaptureFilter) = {
    captureFilter = Some(c)
  }

  // TODO: check if tracing disabled, then also disable storage
  var tracingEnabled = true
  var storageEnabled = false

  var storageHandler: StorageHandler = new NullStorageHandler()

  def setStorageHandler(s: StorageHandler): Unit = {
    storageHandler = s
  }
}
