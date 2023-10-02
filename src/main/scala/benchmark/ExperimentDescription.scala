package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.BenchmarkConfig

object ExperimentParameters {

  abstract class Algorithm

  case class BFS() extends Algorithm
  case class PageRank() extends Algorithm
  case class SSSP() extends Algorithm
  case class WCC() extends Algorithm
  case class LCC() extends Algorithm
}

case class ExperimentDescription(
    // Experiment identifier
    experimentID: String,
    // Dataset to run algorithm on
    dataset: String,
    // Algorithm to run
    algorithm: ExperimentParameters.Algorithm,
    // Run number
    runNr: Long,
    // Directory to store results (local filesystem)
    outputDir: os.Path,
    // Graphalytics benchmark configuration
    benchmarkConfig: BenchmarkConfig,
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

object AlgorithmSerializer {
  def serialize(d: ExperimentParameters.Algorithm): String = {
    d match {
      case ExperimentParameters.BFS()      => "BFS"
      case ExperimentParameters.PageRank() => "PR"
      case ExperimentParameters.SSSP()     => "SSSP"
      case ExperimentParameters.WCC()      => "WCC"
      case ExperimentParameters.LCC()      => "LCC"
      case _                               => throw new NotImplementedError("unknown graph algorithm")
    }
  }

  def deserialize(s: String): ExperimentParameters.Algorithm = {
    s match {
      case "BFS"  => ExperimentParameters.BFS()
      case "PR"   => ExperimentParameters.PageRank()
      case "SSSP" => ExperimentParameters.SSSP()
      case "WCC"  => ExperimentParameters.WCC()
      case "LCC"  => ExperimentParameters.LCC()
      case _      => throw new NotImplementedError("unknown graph algorithm")
    }
  }
}

object ExperimentDescriptionSerializer {
  def serialize(d: ExperimentDescription): ujson.Obj = {
    ujson
      .Obj(
        "id" -> d.experimentID,
        "dataset" -> d.dataset,
        "algorithm" -> AlgorithmSerializer.serialize(d.algorithm),
        "runNr" -> d.runNr,
        "outputDir" -> d.outputDir.toString(),
        "configPath" -> d.benchmarkConfig.path,
        "lineageDir" -> d.lineageDir,
        "setup" -> d.setup
      )
  }

  def deserialize(s: String): ExperimentDescription = {
    val data = ujson.read(s)
    ExperimentDescription(
      experimentID = data("id").str,
      dataset = data("dataset").str,
      algorithm = AlgorithmSerializer.deserialize(data("algorithm").str),
      runNr = data("runNr").str.toLong,
      outputDir = os.Path(data("outputDir").str),
      benchmarkConfig = new BenchmarkConfig(
        data("configPath").str
      ),
      lineageDir = data("lineageDir").str,
      setup = data("setup").str
    )
  }
}
