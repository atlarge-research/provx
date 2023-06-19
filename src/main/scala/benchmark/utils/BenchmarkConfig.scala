package lu.magalhaes.gilles.provxlib
package benchmark.utils

import java.text.SimpleDateFormat
import java.util.Calendar

class BenchmarkConfig(path: String) {
  object RequiredKeys extends Enumeration {
    type RequiredKeys = Value
    // Inputs
    // Where the graphs are stored on HDFS
    val datasetPath = Value("benchmark.datasetPath")

    // Algorithms to run (BFS, PR, WCC, SSP, LCC, PR)
    val algorithms = Value("benchmark.algorithms")

    // Where to store metrics and execution logs
    val experimentsPath = Value("benchmark.experimentsPath")

    // Name of the graphs to run experiments for
    val graphs = Value("benchmark.graphs")

    // Number of repetitions for algorithm and dataset combination
    val repetitions = Value("benchmark.repetitions")

    // Outputs (HDFS)
    // Where to store the lineage information
    val lineagePath = Value("benchmark.lineagePath")

    // Where to store the output of the graph algorithm
    val outputPath = Value("benchmark.outputPath")

    // Where to store Spark event logs
    val sparkLogs = Value("benchmark.sparkLogs")

    // Location of benchmark jar
    val benchmarkJar = Value("runner.jar")
  }
  private lazy val config = SafeConfiguration.fromLocalPath(path).get

  def getPath: String = path
  def validate(): Boolean = {
    val requiredKeys = RequiredKeys.values.map(_.toString)
    val definedKeys = config.getKeys().toSet
    requiredKeys.intersect(definedKeys).size == requiredKeys.size
  }

  var currentExperimentDir: Option[os.Path] = None

  def createCurrentExperimentDir(): Option[os.Path] = {
    val now = Calendar.getInstance().getTime
    val datetimeFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
    val currentExperimentPath = os.Path(experimentsPath) / datetimeFormat.format(now)

    if (os.exists(currentExperimentPath)) {
      None
    } else {
      os.makeDir.all(currentExperimentPath)
      currentExperimentDir = Some(currentExperimentPath)
      currentExperimentDir
    }
  }

  /**
   * Print configuration information for debugging
   */
  def debug(): Unit = {
    println(s"Dataset path: ${datasetPath}")
    println(s"Lineage path: ${lineagePath}")
    println(s"Output  path: ${outputPath}")
    if (currentExperimentDir.isDefined) {
      println(s"Experiments path: ${currentExperimentDir.get}")
    }
    println(s"Repetitions : ${repetitions}")
    println(s"Graphs:     : ${graphs.toSet.mkString(", ")}")
    println(s"Algorithms  : ${algorithms.toSet.mkString(", ")}")
  }

  def datasetPath: String = config.getString(RequiredKeys.datasetPath.toString).get

  def algorithms: Array[String] = config.getStringArray(RequiredKeys.algorithms.toString).map(_.map(_.toLowerCase)).get

  def experimentsPath: String = config.getString(RequiredKeys.experimentsPath.toString).get

  def graphs: Array[String] = config.getStringArray(RequiredKeys.graphs.toString).get

  def repetitions: Int = config.getInt(RequiredKeys.repetitions.toString).get

  def lineagePath: String = config.getString(RequiredKeys.lineagePath.toString).get

  def outputPath: String = config.getString(RequiredKeys.outputPath.toString).get

  def sparkLogs: String = config.getString(RequiredKeys.sparkLogs.toString).get

  def benchmarkJar: String = config.getString(RequiredKeys.benchmarkJar.toString).get
}
