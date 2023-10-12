package lu.magalhaes.gilles.provxlib
package benchmark.configuration

import com.typesafe.config.ConfigRenderOptions
import lu.magalhaes.gilles.provxlib.benchmark.configuration.GraphAlgorithm.GraphAlgorithm
import pureconfig._
import pureconfig.generic.auto._

case class BenchmarkAppConfig(
    // Experiment identifier
    experimentID: String,
    // Dataset to run algorithm on
    dataset: String,
    // Path to dataset to run (usually stored on Hadoop)
    datasetPath: String,
    // Algorithm to run
    algorithm: GraphAlgorithm,
    // Run number
    runNr: Long,
    // Directory to store results (local filesystem)
    outputDir: os.Path,
    // Graphalytics benchmark configuration
    graphalyticsConfigPath: String,
    // Path where to storage lineage
    lineageDir: String,
    // Experiment setup
    setup: String
) {
  override def toString(): String = {
    val sb = new StringBuilder()
    sb.append(s"Experiment ID : ${experimentID}\n")
    sb.append(s"Dataset       : ${dataset}\n")
    sb.append(s"Algorithm     : ${algorithm}\n")
    sb.append(s"Run           : ${runNr}\n")
    sb.append(s"Output dir    : ${outputDir}\n")
    sb.append(s"Lineage dir   : ${lineageDir}\n")
    sb.append(s"Setup         : ${setup}\n")
    sb.toString()
  }
}

object BenchmarkAppConfig {
  implicit val pathReadWriter: ConfigConvert[os.Path] =
    ConfigConvert[String].xmap(os.Path(_), _.toString)

  implicit val graphAlgorithmConverter: ConfigConvert[GraphAlgorithm] =
    ConfigConvert[String].xmap(
      GraphAlgorithm.withName,
      _.toString
    )

  def loadString(contents: String): ConfigReader.Result[BenchmarkAppConfig] =
    ConfigSource.string(contents).load[BenchmarkAppConfig]

  def write(config: BenchmarkAppConfig): String = {
    ConfigWriter[BenchmarkAppConfig]
      .to(config)
      .render(ConfigRenderOptions.concise())
  }
}
