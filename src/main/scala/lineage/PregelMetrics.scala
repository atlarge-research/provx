package lu.magalhaes.gilles.provxlib
package lineage

import java.io._

class PregelMetrics {

  lazy val totalMessages = iterations.map(_.getMessageCount()).sum

  var iterations: List[PregelIterationMetrics] = List.empty

  def getIterations(): List[PregelIterationMetrics] = iterations
  def updateMetrics(metrics: PregelIterationMetrics): Unit = {
    iterations = iterations :+ metrics
  }

  def saveAsTextFile(path: String): Unit = {
    val pw = new PrintWriter(new File(path))
    for ((iterationMetrics: PregelIterationMetrics, idx: Int) <- iterations.zipWithIndex) {
      pw.write(s"${idx}\t${iterationMetrics.getMessageCount()}\n")
    }
    pw.close()
  }
}
