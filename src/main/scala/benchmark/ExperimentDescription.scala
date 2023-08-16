package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.BenchmarkConfig

object ExperimentParameters {

  abstract class Algorithm

  case class BFS() extends Algorithm
  case class PageRank() extends Algorithm
  case class SSSP() extends Algorithm
  case class WCC() extends Algorithm
}

case class ExperimentDescription(
    // Experiment identifier
    experimentID: String,
    // Dataset to run algorithm on
    dataset: String,
    // Algorithm to run
    algorithm: ExperimentParameters.Algorithm,
    // Lineage enabled")
    lineageActive: Boolean,
    // Run number
    runNr: Long,
    // Directory to store results (local filesystem)
    outputDir: os.Path,
    // Graphalytics benchmark configuration
    benchmarkConfig: BenchmarkConfig
)

object AlgorithmSerializer {
  def serialize(d: ExperimentParameters.Algorithm): String = {
    d match {
      case ExperimentParameters.BFS()      => "BFS"
      case ExperimentParameters.PageRank() => "PR"
      case ExperimentParameters.SSSP()     => "SSSP"
      case ExperimentParameters.WCC()      => "WCC"
      case _                               => throw new NotImplementedError("unknown graph algorithm")
    }
  }

  def deserialize(s: String): ExperimentParameters.Algorithm = {
    s match {
      case "BFS"  => ExperimentParameters.BFS()
      case "PR"   => ExperimentParameters.PageRank()
      case "SSSP" => ExperimentParameters.SSSP()
      case "WCC"  => ExperimentParameters.WCC()
      case _      => throw new NotImplementedError("unknown graph algorithm")
    }
  }
}

object ExperimentDescriptionSerializer {
  def serialize(d: ExperimentDescription): String = {
    ujson
      .Obj(
        "id" -> d.experimentID,
        "dataset" -> d.dataset,
        "algorithm" -> AlgorithmSerializer.serialize(d.algorithm),
        "lineageActive" -> d.lineageActive,
        "runNr" -> d.runNr,
        "outputDir" -> d.outputDir.toString(),
        "configPath" -> d.benchmarkConfig.path
      )
      .toString()
  }

  def deserialize(s: String): ExperimentDescription = {
    val data = ujson.read(s)
    ExperimentDescription(
      experimentID = data("id").str,
      dataset = data("dataset").str,
      algorithm = AlgorithmSerializer.deserialize(data("algorithm").str),
      lineageActive = data("lineageActive").bool,
      runNr = data("runNr").str.toLong,
      outputDir = os.Path(data("outputDir").str),
      benchmarkConfig = new BenchmarkConfig(
        data("configPath").str
      )
    )
  }
}
