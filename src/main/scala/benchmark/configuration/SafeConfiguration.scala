package lu.magalhaes.gilles.provxlib
package benchmark.configuration

import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer

class SafeConfiguration(config: PropertiesConfiguration) {

  def getKeys(): List[String] = {
    val iter = config.getKeys()
    val keys = ArrayBuffer[String]()
    while (iter.hasNext) {
      val v = iter.next()
      keys += v
    }
    keys.toList
  }

  def getInt(key: String): Option[Int] = {
    try {
      Some(config.getInt(key))
    } catch {
      case _: Throwable => None
    }
  }

  def getString(key: String): Option[String] = {
    try {
      Some(config.getString(key))
    } catch {
      case _: Throwable => None
    }
  }

  def getStringArray(key: String): Option[Array[String]] = {
    try {
      Some(config.getStringArray(key))
    } catch {
      case _: Throwable => None
    }
  }
}

object SafeConfiguration {

  def fromLocalPath(path: String): Option[SafeConfiguration] = {
    try {
      val propConfig = new PropertiesConfiguration(path)
      Some(new SafeConfiguration(propConfig))
    } catch {
      case e: ConfigurationException =>
        println(e)
        None
    }
  }

  def fromHadoop(
      path: String,
      hadoopConfig: HadoopConfiguration
  ): Option[SafeConfiguration] = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConfig)
    val in = fs.open(hadoopPath)

    try {
      val propConfig = new PropertiesConfiguration()
      propConfig.load(in)
      Some(new SafeConfiguration(propConfig))
    } catch {
      case e: ConfigurationException =>
        println(e)
        None
    }
  }
}
