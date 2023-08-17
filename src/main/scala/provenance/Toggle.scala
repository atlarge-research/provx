package lu.magalhaes.gilles.provxlib
package provenance

class Toggle(initialValue: Boolean) {
  private var state = initialValue

  def isEnabled: Boolean = state

  def enable(): Unit = {
    state = true
  }

  def disable(): Unit = {
    state = false
  }
}
