package lu.magalhaes.gilles.provxlib
package benchmark.configuration

import benchmark.configuration.ExperimentSetup.ExperimentSetup
import benchmark.configuration.GraphAlgorithm.GraphAlgorithm
import provenance.storage.StorageFormat

import com.typesafe.config.ConfigRenderOptions
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.generic.ProductHint

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
    // Storage format
    storageFormat: StorageFormat,
    // Directory to store results (local filesystem)
    outputDir: os.Path,
    // Graphalytics benchmark configuration
    graphalyticsConfigPath: String,
    // Path where to storage lineage
    lineageDir: String,
    // Number of executors for this job
    numExecutors: Long,
    // Experiment setup
    setup: ExperimentSetup
) {
  override def toString: String =
    s"""Num executors : $numExecutors
       |Experiment ID : $experimentID
       |Dataset       : $dataset
       |Algorithm     : $algorithm
       |Run           : $runNr
       |Output dir    : $outputDir
       |Lineage dir   : $lineageDir
       |Setup         : $setup""".stripMargin
}

object BenchmarkAppConfig {
  implicit val pathReadWriter: ConfigConvert[os.Path] =
    ConfigConvert[String].xmap(os.Path(_), _.toString)

  implicit val graphAlgorithmConverter: ConfigConvert[GraphAlgorithm] =
    ConfigConvert[String].xmap(
      GraphAlgorithm.withName,
      _.toString
    )

  implicit val experimentSetupConverter: ConfigConvert[ExperimentSetup] =
    ConfigConvert[String].xmap(
      ExperimentSetup.withName,
      _.toString
    )

  implicit val storageFormatConverter: ConfigConvert[StorageFormat] =
    ConfigConvert[String].xmap(
      StorageFormat.fromString,
      _.toString
    )

  implicit def productHint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def loadString(contents: String): ConfigReader.Result[BenchmarkAppConfig] =
    ConfigSource.string(contents).load[BenchmarkAppConfig]

  def write(config: BenchmarkAppConfig): String = {
    ConfigWriter[BenchmarkAppConfig]
      .to(config)
      .render(ConfigRenderOptions.concise())
  }
}
