package lu.magalhaes.gilles.provxlib
package benchmark.configuration

import pureconfig.{ConfigReader, ConfigSource}

import java.io.{File, PrintWriter}
import scala.io.Source

trait ConfigLoader[T] {
  def loadString(contents: String): ConfigReader.Result[T] =
    load(ConfigSource.file(createTempPropertiesFile(contents)))

  def loadFile(path: String): ConfigReader.Result[T] =
    load(ConfigSource.file(path))

  def loadResource(name: String): ConfigReader.Result[T] =
    loadString(Source.fromResource(name).mkString)

  def createTempPropertiesFile(contents: String): String = {
    val f = File.createTempFile(
      System.getProperty("java.io.tmpdir") + "configuration-",
      ".properties"
    )
    f.deleteOnExit()
    new PrintWriter(f) {
      try {
        write(contents)
      } finally {
        close()
      }
    }
    f.getAbsolutePath
  }

  def load(configSource: ConfigSource): ConfigReader.Result[T]
}
