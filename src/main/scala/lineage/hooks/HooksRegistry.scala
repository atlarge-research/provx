package lu.magalhaes.gilles.provxlib
package lineage.hooks

class HooksRegistry {

  private val defaultHooks: Seq[PregelEventHook] = Seq(
    new IterationCounterHook(),
    new IterationTimeHook(),
    new PregelTimeHook()
  )

  private val hooks = scala.collection.mutable.ArrayBuffer.empty[PregelEventHook]

  def register(hook: PregelEventHook): Unit = {
    hooks += hook
  }

  def deregister(hook: PregelEventHook): Unit = {
    val newHooks = hooks.filter(_ ne hook)
    hooks.clear()
    hooks ++= newHooks
  }

  def allHooks: Iterable[PregelEventHook] = defaultHooks ++ hooks

  def clear(): Unit = {
    hooks.clear()
  }

}
