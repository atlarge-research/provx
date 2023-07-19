package lu.magalhaes.gilles.provxlib
package lineage.hooks

class HooksRegistry {

  private val hooks = scala.collection.mutable.ArrayBuffer(
    new IterationCounterHook(),
    new IterationTimeHook(),
    new PregelTimeHook()
  )

  def register(hook: PregelEventHook): Unit = {
    hooks += hook
  }

  def deregister(hook: PregelEventHook): Unit = {
    val newHooks = hooks.filter(_ ne hook)
    hooks.clear()
    hooks ++= newHooks
  }

  def allHooks: Iterable[PregelEventHook] = hooks

  def clear(): Unit = {
    hooks.clear()
  }

}
