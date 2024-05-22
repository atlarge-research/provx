package lu.magalhaes.gilles.provxlib
package benchmark.configuration

import benchmark.configuration.ExperimentSetup.ExperimentSetup
import benchmark.configuration.GraphAlgorithm.GraphAlgorithm
import benchmark.utils.{TextUtils, TimeUtils}
import provenance.storage.StorageFormat

import com.typesafe.config.ConfigRenderOptions
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.generic.ProductHint
import pureconfig.ConfigReader.Result

import java.text.SimpleDateFormat
import java.util.Calendar

case class RunnerParameters(
    // Inputs
    // Number of repetitions for algorithm and dataset combination
    repetitions: Int,
    // Algorithms to run
    algorithms: Set[GraphAlgorithm],
    // Name of the graphs to run experiments for
    graphs: List[String],
    // Where the graphs are stored on HDFS
    datasetPath: String,
    // Where to store metrics and execution logs
    experimentsPath: String,
    // Which experiment setups to run
    setups: Set[ExperimentSetup],
    // Storage formats
    storageFormats: Set[StorageFormat],
    // Outputs (HDFS)
    // Where to store the lineage information
    lineagePath: String,
    // Where to store the output of the graph algorithm
    outputPath: String,
    // Where to store Spark event logs
    sparkLogs: String,
    // Location of benchmark jar
    jar: String,
    // Benchmark timeout
    timeoutMinutes: Long = 30
)

case class RunnerConfigData(
    runner: RunnerParameters
) {

  /** Print configuration information for debugging
    */
  def debug(): Unit = {
    println(s"""Dataset path: ${runner.datasetPath}
               |Lineage path: ${runner.lineagePath}
               |Output  path: ${runner.outputPath}
               |Repetitions : ${runner.repetitions}
               |Graphs:     : ${runner.graphs.toSet.mkString(", ")}
               |Algorithms  : ${runner.algorithms.mkString(", ")}
               |Setups      : ${runner.setups.mkString(", ")}
               |Max. timeout: ${TextUtils.plural(
      runner.timeoutMinutes,
      "minute"
    )}""".stripMargin)
  }
}

object RunnerConfig extends ConfigLoader[RunnerConfigData] {
  implicit val experimentSetupRW: ConfigConvert[Set[ExperimentSetup]] =
    ConfigConvert[List[String]].xmap(
      _.map(ExperimentSetup.withName).toSet,
      _.map(_.toString).toList
    )

  implicit val graphAlgorithmConverter: ConfigConvert[Set[GraphAlgorithm]] =
    ConfigConvert[List[String]].xmap(
      _.map(GraphAlgorithm.withName).toSet,
      _.map(_.toString).toList
    )

  implicit val storageFormatsConverter: ConfigConvert[Set[StorageFormat]] =
    ConfigConvert[List[String]].xmap(
      _.map(StorageFormat.fromString).toSet,
      _.map(_.toString).toList
    )

  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  override def extension(): String = ".conf"

  def currentExperimentDir(experimentsPath: String): os.Path = {
    val now = Calendar.getInstance().getTime
    val datetimeFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
    os.Path(experimentsPath) / datetimeFormat.format(now)
  }

  def load(configSource: ConfigSource): Result[RunnerConfigData] =
    configSource.load[RunnerConfigData]

  def write(config: RunnerConfigData): String = {
    ConfigWriter[RunnerConfigData]
      .to(config)
      .render(ConfigRenderOptions.concise())
  }
}
