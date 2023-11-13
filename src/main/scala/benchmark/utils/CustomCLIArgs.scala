package lu.magalhaes.gilles.provxlib
package benchmark.utils

import benchmark.configuration.{
  BenchmarkAppConfig,
  RunnerConfig,
  RunnerConfigData
}

import mainargs.TokensReader
import pureconfig._
import pureconfig.generic.auto._

object CustomCLIArgs {
  implicit object BenchmarkAppConfigArg
      extends TokensReader.Simple[BenchmarkAppConfig] {
    def shortName = "path"

    override def alwaysRepeatable: Boolean = false

    override def allowEmpty: Boolean = false

    def read(strs: Seq[String]): Either[String, BenchmarkAppConfig] = {
      BenchmarkAppConfig.loadString(strs.head.replaceAll("\\\"", "\"")) match {
        case Left(value)  => Left(value.toString())
        case Right(value) => Right(value)
      }
    }
  }

  implicit object RunnerCLIArg extends TokensReader.Simple[RunnerConfigData] {
    def shortName = "config"

    override def alwaysRepeatable: Boolean = false

    override def allowEmpty: Boolean = false

    def read(strs: Seq[String]): Either[String, RunnerConfigData] = {
      RunnerConfig.loadFile(strs.head) match {
        case Left(value)  => Left(value.toString())
        case Right(value) => Right(value)
      }
    }
  }

  implicit object PathRead extends TokensReader.Simple[os.Path] {
    def shortName = "path"

    override def alwaysRepeatable: Boolean = false

    override def allowEmpty: Boolean = false

    def read(strs: Seq[String]): Right[Nothing, os.Path] = Right(
      os.Path(strs.head)
    )
  }
}
