package lu.magalhaes.gilles.provxlib
package lineage

object LineageContext {

  // TODO(gm): investigate whether it's useful to have a global lineage context

  private var tracingEnabled = false
  def isTracingEnabled: Boolean = tracingEnabled

  def enableTracing(): Unit = {
    tracingEnabled = true
  }

  def disableTracing(): Unit = {
    tracingEnabled = false
  }
}
