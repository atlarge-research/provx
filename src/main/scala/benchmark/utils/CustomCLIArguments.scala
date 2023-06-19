package lu.magalhaes.gilles.provxlib
package benchmark.utils

import mainargs.TokensReader

object CustomCLIArguments {
  implicit object BenchmarkConfigArg extends TokensReader.Simple[BenchmarkConfig] {
    def shortName = "path"

    override def alwaysRepeatable: Boolean = false

    override def allowEmpty: Boolean = false

    def read(strs: Seq[String]): Right[Nothing, BenchmarkConfig] = Right(new BenchmarkConfig(strs.head))
  }

  implicit object PathRead extends TokensReader.Simple[os.Path] {
    def shortName = "path"

    override def alwaysRepeatable: Boolean = false

    override def allowEmpty: Boolean = false

    def read(strs: Seq[String]): Right[Nothing, os.Path] = Right(os.Path(strs.head))
  }
}
