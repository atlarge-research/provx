package lu.magalhaes.gilles.provxlib
package utils

import java.io.{BufferedWriter, File, FileWriter}

object FileUtils {

  def writeFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
    }
    bw.close()
  }
}
