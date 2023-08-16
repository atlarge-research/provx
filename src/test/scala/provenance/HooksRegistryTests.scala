package lu.magalhaes.gilles.provxlib
package provenance

import provenance.hooks.{HooksRegistry, CounterHook}

import org.scalatest.funsuite.AnyFunSuite

class HooksRegistryTests extends AnyFunSuite {
  test("Register a hook") {
    val registry = new HooksRegistry()
    val originalSize = registry.all.size
    val hook = CounterHook()
    registry.register(hook)
    assert(registry.all.size == originalSize + 1)
  }

  test("Deregister a hook") {
    val registry = new HooksRegistry()
    val originalSize = registry.all.size
    val hook = CounterHook()
    registry.register(hook)
    assert(registry.all.size == originalSize + 1)
    registry.deregister(hook)
    assert(registry.all.size == originalSize)
  }

  test("Clear all user hooks") {
    val registry = new HooksRegistry()
    val originalSize = registry.all.size
    val hook = CounterHook()

    registry.register(hook)
    assert(registry.all.size == originalSize + 1)

    registry.clear()
    // Only default hooks are left
    assert(registry.all.isEmpty)
  }
}
