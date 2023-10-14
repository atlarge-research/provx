package lu.magalhaes.gilles.provxlib
package benchmark.configuration

import benchmark.configuration.GraphAlgorithm.GraphAlgorithm
import benchmark.utils.TextUtils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import pureconfig._
import pureconfig.generic.auto._

import scala.io.Source

case class EdgeProperties(names: String, types: String)

case class BFS(sourceVertex: Int)

case class CDLP(maxIterations: Int)

case class LCC()

case class PR(dampingFactor: Double, numIterations: Int)

case class SSSP(weightProperty: String, sourceVertex: Int)

case class WCC()

case class GraphConfig(
    algorithms: Set[GraphAlgorithm],
    vertexFile: String,
    edgeFile: String,
    meta: Meta,
    directed: Boolean,
    edgeProperties: EdgeProperties,
    bfs: Option[BFS],
    cdlp: Option[CDLP],
    lcc: Option[LCC],
    pr: Option[PR],
    sssp: Option[SSSP],
    wcc: Option[WCC]
)

case class Meta(vertices: Int, edges: Int)

case class GraphalyticsConfigData(
    graph: Map[String, GraphConfig]
)

object GraphalyticsConfig extends ConfigLoader[GraphConfig] {
  implicit val graphAlgorithmConverter: ConfigReader[Set[GraphAlgorithm]] =
    ConfigReader[String].map(
      TextUtils.toStringsList(_).map(GraphAlgorithm.fromString).toSet
    )

  def loadHadoop(
      path: String
  ): GraphConfig = {
    val realPath = if (path.startsWith("/")) {
      s"file://${path}"
    } else {
      path
    }
    val hadoopPath = new Path(realPath)
    val fs = hadoopPath.getFileSystem(new Configuration())
    val in = fs.open(hadoopPath)
    val contents = Source.fromInputStream(in).mkString
    in.close()

    // Necessary to convince PureConfig that Graphalytics configuration files are in properties format.
    val filename = createTempPropertiesFile(contents)

    loadFile(filename) match {
      case Left(errors) =>
        throw new Error(s"Unable to parse ${path} configuration: ${errors}")
      case Right(value) => value
    }
  }

  def load(
      configSource: ConfigSource
  ): ConfigReader.Result[GraphConfig] = {
    configSource
      .load[GraphalyticsConfigData]
      .map(conf => {
        conf.graph(conf.graph.keys.head)
      })
  }
}
