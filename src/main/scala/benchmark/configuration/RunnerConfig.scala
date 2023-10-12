package lu.magalhaes.gilles.provxlib
package benchmark.configuration

import com.typesafe.config.ConfigRenderOptions
import lu.magalhaes.gilles.provxlib.benchmark.configuration.ExperimentSetup.ExperimentSetup
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.generic.ProductHint
import pureconfig.ConfigReader.Result

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

case class RunnerParameters(
    // Inputs
    // Number of repetitions for algorithm and dataset combination
    repetitions: Int,
    // Algorithms to run (BFS, PR, WCC, SSP, LCC, PR)
    // TODO: read these in as lowercase
    algorithms: List[String],
    // Name of the graphs to run experiments for
    graphs: List[String],
    // Where the graphs are stored on HDFS
    datasetPath: String,
    // Where to store metrics and execution logs
    experimentsPath: String,
    // Which experiment setups to run
    setups: List[ExperimentSetup],
    // Outputs (HDFS)
    // Where to store the lineage information
    lineagePath: String,
    // Where to store the output of the graph algorithm
    outputPath: String,
    // Where to store Spark event logs
    sparkLogs: String,
    // Location of benchmark jar
    jar: String
)

case class RunnerConfigData(
    runner: RunnerParameters
) {

  /** Print configuration information for debugging
    */
  def debug(): Unit = {
    println(s"Dataset path: ${runner.datasetPath}")
    println(s"Lineage path: ${runner.lineagePath}")
    println(s"Output  path: ${runner.outputPath}")
    println(s"Repetitions : ${runner.repetitions}")
    println(s"Graphs:     : ${runner.graphs.toSet.mkString(", ")}")
    println(s"Algorithms  : ${runner.algorithms.toSet.mkString(", ")}")
    println(s"Setups      : ${runner.setups.toSet.mkString(", ")}")
  }
}

object RunnerConfig extends ConfigLoader[RunnerConfigData] {
  implicit val stringListReader: ConfigReader[List[String]] =
    ConfigReader[String].map(
      _.split(",").toList
        .map(_.trim)
        .filterNot(_.startsWith("#"))
        .filterNot(_.isEmpty)
    )

  implicit val experimentSetupReader: ConfigReader[List[ExperimentSetup]] =
    ConfigReader[String].map(
      _.split(",").toList
        .map(_.trim)
        .filterNot(_.startsWith("#"))
        .filterNot(_.isEmpty)
        .map(v => ExperimentSetup.withName(v.trim))
    )

  implicit val stringListWriter: ConfigWriter[List[String]] =
    ConfigWriter[String].contramap[List[String]](_.mkString(", "))

  implicit val experimentSetupWriter: ConfigWriter[List[ExperimentSetup]] =
    ConfigWriter[String].contramap[List[ExperimentSetup]](
      _.map(_.toString).mkString(", ")
    )

  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

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
