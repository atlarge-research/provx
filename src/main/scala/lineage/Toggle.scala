package lu.magalhaes.gilles.provxlib
package lineage

class Toggle {
  private var state = false

  def isEnabled: Boolean = state

  def enable(): Unit = {
    state = true
  }

  def disable(): Unit = {
    state = false
  }
}
