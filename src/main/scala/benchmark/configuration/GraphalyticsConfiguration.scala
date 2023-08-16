package lu.magalhaes.gilles.provxlib
package benchmark.configuration

import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}

class GraphalyticsConfiguration(
    hadoopConfig: HadoopConfiguration,
    path: String
) {

  private val config = SafeConfiguration.fromHadoop(path, hadoopConfig).get
  val datasetName = path.split("/").last.split("\\.").head

  def algorithms(): Option[Array[String]] = {
    try {
      val value = config
        .getStringArray(s"graph.${datasetName}.algorithms")
        .get
        .map(_.toLowerCase)
      Some(value)
    } catch {
      case _: Throwable => None
    }
  }

  def withCheck[T](algorithm: String, f: SafeConfiguration => T): T = {
    require(algorithms().isDefined && algorithms().get.contains(algorithm))
    f(config)
  }

  def bfsSourceVertex(): Int = withCheck(
    "bfs",
    c => {
      c.getInt(s"graph.${datasetName}.bfs.source-vertex")
    }
  ).get

  def pageRankIterations(): Int = withCheck(
    "pr",
    c => {
      c.getInt(s"graph.${datasetName}.pr.num-iterations")
    }
  ).get

  def ssspSourceVertex(): Long = withCheck(
    "sssp",
    c => {
      c.getInt(s"graph.${datasetName}.sssp.source-vertex")
    }
  ).get
}
