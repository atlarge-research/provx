package lu.magalhaes.gilles.provxlib
package benchmark.configuration

object GraphAlgorithm extends Enumeration {
  type GraphAlgorithm = Value
  val BFS, PageRank, WCC, SSSP = Value
}
