package lu.magalhaes.gilles.provxlib
package utils

class BenchmarkConfig(path: String) {
  private val config = ConfigurationUtils.load(path)

  // Inputs
  // Where the graphs are stored on HDFS
  def datasetPath: Option[String] =
    ConfigurationUtils.getString(config.get, "benchmark.datasetPath")

  // Algorithms to run (BFS, PR, WCC, SSP, LCC, PR)
  def algorithms: Option[Array[String]] =
    ConfigurationUtils.getStringArray(config.get, "benchmark.algorithms")
      .map(_.map(_.toLowerCase))

  // Where to store metrics and Spark logs
  def experimentsPath: Option[String] =
    ConfigurationUtils.getString(config.get, "benchmark.experimentsPath")

  // Name of the graphs to run experiments for
  def graphs: Option[Array[String]] =
    ConfigurationUtils.getStringArray(config.get, "benchmark.graphs")

  // Number of repetitions for algorithm and dataset combination
  def repetitions: Option[Int] =
    ConfigurationUtils.getInt(config.get, "benchmark.repetitions")

  // Outputs (HDFS)
  // Where to store the lineage information
  def lineagePath: Option[String] =
    ConfigurationUtils.getString(config.get, "benchmark.lineagePath")

  // Where to store the output of the graph algorithm
  def outputPath: Option[String] =
    ConfigurationUtils.getString(config.get, "benchmark.outputPath")

  def sparkLogs: Option[String] =
    ConfigurationUtils.getString(config.get, "benchmark.spark-logs")

}
