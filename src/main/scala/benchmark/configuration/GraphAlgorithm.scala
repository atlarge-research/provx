package lu.magalhaes.gilles.provxlib
package benchmark.configuration

object GraphAlgorithm extends Enumeration {
  type GraphAlgorithm = Value
  val BFS, PageRank, WCC, SSSP, CDLP, LCC = Value

  def fromString(value: String): GraphAlgorithm.Value = {
    GraphAlgorithm.withName(value.toLowerCase() match {
      case "bfs"             => "BFS"
      case "pr" | "pagerank" => "PageRank"
      case "wcc"             => "WCC"
      case "sssp"            => "SSSP"
      case "lcc"             => "LCC"
      case "cdlp"            => "CDLP"
      case _ =>
        throw new NotImplementedError(
          "unknown graph algorithm in Graphalytics configuration"
        )

    })
  }
}
