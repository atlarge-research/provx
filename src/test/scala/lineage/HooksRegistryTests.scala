package lu.magalhaes.gilles.provxlib
package lineage

import lineage.hooks.{HooksRegistry, IterationCounterHook}

import org.scalatest.funsuite.AnyFunSuite

class HooksRegistryTests extends AnyFunSuite {
  test("Register a hook") {
    val registry = new HooksRegistry()
    val hook = new IterationCounterHook()
    registry.register(hook)
    assert(registry.allHooks.size == 4)
  }

  test("Deregister a hook") {
    val registry = new HooksRegistry()
    val hook = new IterationCounterHook()
    registry.register(hook)
    assert(registry.allHooks.size == 4)
    registry.deregister(hook)
    assert(registry.allHooks.size == 3)
  }

  test("Clear all user hooks") {
    val registry = new HooksRegistry()

    val hook = new IterationCounterHook()
    registry.register(hook)
    assert(registry.allHooks.size == 4)

    registry.clear()

    // Only default hooks are left
    assert(registry.allHooks.size == 3)
  }
}
