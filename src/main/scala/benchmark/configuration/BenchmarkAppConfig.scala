package lu.magalhaes.gilles.provxlib
package benchmark.configuration

import lu.magalhaes.gilles.provxlib.benchmark.configuration.GraphAlgorithm.{
  benchmarkAppConfigReader,
  BFS
}
import upickle.default.{macroRW, readwriter, ReadWriter => RW}
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.generic.semiauto.deriveReader

sealed abstract class GraphAlgorithm

object GraphAlgorithm {
  implicit val benchmarkAppConfigReader: ConfigReader[GraphAlgorithm] =
    deriveReader[GraphAlgorithm]
  case class BFS() extends GraphAlgorithm

  object BFS {
    implicit val rw: RW[BFS] = macroRW
  }
  case class PageRank() extends GraphAlgorithm

  object PageRank {
    implicit val rw: RW[PageRank] = macroRW
  }
  case class SSSP() extends GraphAlgorithm

  object SSSP {
    implicit val rw: RW[SSSP] = macroRW
  }
  case class WCC() extends GraphAlgorithm

  object WCC {
    implicit val rw: RW[WCC] = macroRW
  }

  implicit val rw: RW[GraphAlgorithm] = RW.merge(
    BFS.rw,
    PageRank.rw,
    SSSP.rw,
    WCC.rw
  )
}

case class BenchmarkAppConfig(
    // Experiment identifier
    experimentID: String,
    // Dataset to run algorithm on
    dataset: String,
    // Path to dataset to run (usually stored on Hadoop)
    datasetPath: os.Path,
    // Algorithm to run
    algorithm: GraphAlgorithm,
    // Run number
    runNr: Long,
    // Directory to store results (local filesystem)
    outputDir: os.Path,
    // Graphalytics benchmark configuration
    graphalyticsConfigPath: os.Path,
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
  implicit val pathRW: RW[os.Path] =
    readwriter[String].bimap[os.Path](
      _.toString(),
      os.Path(_)
    )
  implicit val rw: RW[BenchmarkAppConfig] = macroRW

  implicit val pathConfigReader: ConfigReader[os.Path] =
    ConfigReader[String].map(os.Path(_))
  implicit val pathConfigWriter: ConfigWriter[os.Path] =
    ConfigWriter[String].contramap(_.toString())

  def loadString(contents: String): ConfigReader.Result[BenchmarkAppConfig] =
    ConfigSource.string(contents).load[BenchmarkAppConfig]
}
