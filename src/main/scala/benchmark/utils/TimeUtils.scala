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
    val seconds = duration.toSeconds - minutes * 60 - hours * 3600 - days * 86400

    val sb = new StringBuilder()

    if (days > 0) {
      sb.append(s"${days} day${if (days != 1) "s" else ""} ")
    }
    if (hours > 0) {
      sb.append(s"${hours} hour${if (hours != 1) "s" else ""} ")
    }
    if (minutes > 0) {
      sb.append(s"${minutes} minute${if (minutes != 1) "s" else ""} ")
    }
    sb.append(s"${seconds} second${if (seconds != 1) "s" else ""}")

    sb.toString()
  }
}
