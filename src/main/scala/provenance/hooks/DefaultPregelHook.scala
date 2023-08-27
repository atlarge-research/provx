package lu.magalhaes.gilles.provxlib
package provenance.hooks

case class DefaultPregelHook()
    extends PregelHook(
      Seq(),
      Seq(CounterHook())
    )
