package lu.magalhaes.gilles.provxlib
package utils

import org.apache.commons.configuration.{Configuration, ConfigurationException, PropertiesConfiguration}

object ConfigurationUtils {

  def load(path: String): Option[Configuration] = {
    println(s"Loading configuration ${path}")
    try {
      Some(new PropertiesConfiguration(path))
    } catch {
      case e: ConfigurationException =>
        println(e)
        None
    }
  }

  def getInt(config: Configuration, key: String): Option[Int] = {
    try {
      Some(config.getInt(key))
    } catch {
      case _: Throwable => None
    }
  }

  def getString(config: Configuration, key: String): Option[String] = {
    try {
      Some(config.getString(key))
    } catch {
      case _: Throwable => None
    }
  }

  def getStringArray(config: Configuration, key: String): Option[Array[String]] = {
    try {
      Some(config.getStringArray(key))
    } catch {
      case _: Throwable => None
    }
  }
}
