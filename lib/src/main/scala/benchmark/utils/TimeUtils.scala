package lu.magalhaes.gilles.provxlib
package benchmark.utils

import scala.concurrent.duration._

object TimeUtils {

  def timed[R](f: => R): (R, Long) = {
    val startTime = System.nanoTime()
    val res = f
    val endTime = System.nanoTime()
    (res, endTime - startTime)
  }
  def formatNanoseconds(nanoseconds: Long): String = {
    val duration = Duration(nanoseconds, NANOSECONDS)
    val days = duration.toDays
    val hours = duration.toHours - days * 24
    val minutes = duration.toMinutes - hours * 60 - days * 1440
    val seconds =
      duration.toSeconds - minutes * 60 - hours * 3600 - days * 86400

    List(
      (days, "day"),
      (hours, "hour"),
      (minutes, "minute"),
      (seconds, "second")
    )
      .filter(v => v._1 > 0)
      .map(v => TextUtils.plural(v._1, v._2))
      .mkString(" ")
  }
}
