package lu.magalhaes.gilles.provxlib
package utils

import org.apache.commons.configuration.{Configuration, ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.Path

class GraphalyticsConfiguration(hadoopConfig: HadoopConfiguration, path: String) {

  private val config = load()
  val datasetName = path.split("/").last.split("\\.").head
  private def load(): Option[Configuration] = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConfig)
    val in = fs.open(hadoopPath)
    println(s"Loading ${hadoopPath}")
    try {
      val propConfig = new PropertiesConfiguration()
      propConfig.load(in)
      Some(propConfig)
    } catch {
      case e: ConfigurationException =>
        println(e)
        None
    }
  }

  def algorithms(): Option[Array[String]] = {
    require(config.isDefined, "Configuration parsing failed")
    try {
      val value = config.get.getStringArray(s"graph.${datasetName}.algorithms")
        .map(_.toLowerCase)

      Some(value)
    } catch {
      case _: Throwable => None
    }
  }

  def withCheck[T](algorithm: String, f: Configuration => T): T = {
    require(config.isDefined, "Configuration parsing failed")
    require(algorithms().isDefined && algorithms().get.contains(algorithm))
    f(config.get)
  }

  def bfsSourceVertex(): Int = withCheck("bfs", c => {
    ConfigurationUtils.getInt(c, s"graph.${datasetName}.bfs.source-vertex")
  }).get

  def pageRankIterations(): Int = withCheck("pr", c => {
    ConfigurationUtils.getInt(c, s"graph.${datasetName}.pr.num-iterations")
  }).get

  def ssspSourceVertex(): Long = withCheck("sssp", c => {
    ConfigurationUtils.getInt(c, s"graph.${datasetName}.sssp.source-vertex")
  }).get
}
