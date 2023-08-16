package lu.magalhaes.gilles.provxlib
package lineage.hooks

case class DefaultPregelHook()
    extends PregelHook(
      Seq(TimeHook("pregelTime")),
      Seq(TimeHook("iterationTime"), CounterHook())
    )
