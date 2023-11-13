package lu.magalhaes.gilles.provxlib
package benchmark.utils

object TextUtils {
  def toStringsList(input: String): List[String] = {
    input
      .split(",")
      .toList
      .map(_.trim)
      .filterNot(_.startsWith("#"))
      .filterNot(_.isEmpty)
      .map(_.trim)
  }

  def fromTList[T](input: List[T]): String = {
    input.map(_.toString).mkString(", ")
  }
}
