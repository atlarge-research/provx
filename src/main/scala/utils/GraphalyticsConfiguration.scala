package lu.magalhaes.gilles.provxlib
package utils

import org.apache.commons.configuration.{Configuration, ConfigurationException, PropertiesConfiguration}

class GraphalyticsConfiguration(path: String) {

  private val config = load()
  val datasetName = path.split("/").last.split("\\.").head
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

  def algorithms(): Option[Array[String]] = {
    require(config.isDefined, "Configuration parsing failed")
    try {
      Some(config.get.getStringArray(s"graph.${datasetName}.algorithms"))
    } catch {
      case _: Throwable => None
    }
  }

  private def checks(algorithm: String) = {
    require(config.isDefined, "Configuration parsing failed")
    require(algorithms().isDefined && algorithms().get.contains(algorithm))
  }

  def bfsSourceVertex(): Int = {
    checks("bfs")
    config.get.getInt(s"graph.${datasetName}.bfs.source-vertex")
  }

  def pageRankIterations(): Int = {
    checks("pr")
    config.get.getInt(s"graph.${datasetName}.pr.num-iterations")
  }

  def ssspSourceVertex(): Long = {
    checks("sssp")
    config.get.getInt(s"graph.${datasetName}.sssp.source-vertex")
  }
}
