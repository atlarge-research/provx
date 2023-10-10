package lu.magalhaes.gilles.provxlib
package benchmark.configuration

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
    // Algorithms to run (BFS, PR, WCC, SSP, LCC, PR)
    // TODO: read these in as lowercase
    algorithms: List[String],
    // Name of the graphs to run experiments for
    graphs: List[String],
    // Where the graphs are stored on HDFS
    datasetPath: String,
    // Where to store metrics and execution logs
    experimentsPath: String,
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
  }
}

object RunnerConfig extends ConfigLoader[RunnerConfigData] {
  implicit val stringListReader: ConfigReader[List[String]] =
    ConfigReader[String].map(_.split(",").toList.map(_.trim))

  implicit def hint[T]: ProductHint[T] =
    ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def currentExperimentDir(experimentsPath: String): os.Path = {
    val now = Calendar.getInstance().getTime
    val datetimeFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
    os.Path(experimentsPath) / datetimeFormat.format(now)
  }

  def load(configSource: ConfigSource): Result[RunnerConfigData] =
    configSource.load[RunnerConfigData]
}
