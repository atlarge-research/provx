package lu.magalhaes.gilles.provxlib
package utils

import org.apache.commons.configuration.{Configuration, ConfigurationException, PropertiesConfiguration}

class BenchmarkConfig(path: String) {
  private val config = load()
  private def load(): Option[Configuration] = {
    println(s"Loading ${path}")
    var configuration: Configuration = null
    try {
      Some(new PropertiesConfiguration(path))
    } catch {
      case e: ConfigurationException =>
        println(e)
        None
    }
  }

//  val datasetPathPrefix = args(0) // /var/scratch/gmo520/thesis/benchmark/graphs/xs
//  val metricsPathPrefix = args(1) // /var/scratch/gmo520/thesis/results
//  val lineagePathPrefix = args(2) // /local/gmo520

  def datasetPath: Option[String] = getString("benchmark.datasetPath")

  def metricsPath: Option[String] = getString("benchmark.metricsPath")

  def lineagePath: Option[String] = getString("benchmark.lineagePath")

  def outputPath: Option[String] = getString("benchmark.outputPath")

  def graphs: Option[Array[String]] = getStringArray("benchmark.graphs")


  private def getString(key: String): Option[String] = {
    try {
      Some(config.get.getString(key))
    } catch {
      case _: Throwable => None
    }
  }

  private def getStringArray(key: String): Option[Array[String]] = {
    try {
      Some(config.get.getStringArray(key))
    } catch {
      case _: Throwable => None
    }
  }
}
