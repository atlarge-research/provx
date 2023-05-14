package lu.magalhaes.gilles.provxlib
package utils

import scala.io.Source

object GraphalyticsOutputReader {
  def readFile[T](path: String, func: String => T, maxValue: T): Array[(Long, T)] = {
    val bufferedSource = Source.fromURL(path)
    val result = bufferedSource
      .getLines()
      .map(x => {
        val vertexId = x.split(" ").head.trim.toLong
        val value = if (x.split(" ").last.trim == "infinity") {
          maxValue
        } else {
          func(x.split(" ").last.trim)
        }
        (vertexId, value)
      })
      .toArray
    bufferedSource.close()
    result
  }
  def readFloat(path: String): Array[(Long, Float)] =
    readFile(path, x => x.toFloat, Float.PositiveInfinity)

  def readLong(path: String): Array[(Long, Long)] =
    readFile(path, x => x.toLong, Long.MaxValue)
}
