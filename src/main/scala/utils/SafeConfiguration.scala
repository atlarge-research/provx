package lu.magalhaes.gilles.provxlib
package utils

import org.apache.commons.configuration.{ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.Path

class SafeConfiguration(config: PropertiesConfiguration) {

  def getKeys(): List[String] = {
    List()
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

  def fromLocalPath(path: String) : Option[SafeConfiguration] = {
    println(s"Loading configuration ${path}")
    try {
      val propConfig = new PropertiesConfiguration(path)
      Some(new SafeConfiguration(propConfig))
    } catch {
      case e: ConfigurationException =>
        println(e)
        None
    }
  }

  def fromHadoop(path: String, hadoopConfig: HadoopConfiguration): Option[SafeConfiguration] = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConfig)
    val in = fs.open(hadoopPath)
    println(s"Loading configuration ${hadoopPath}")
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
