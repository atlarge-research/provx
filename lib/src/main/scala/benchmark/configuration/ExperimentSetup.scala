package lu.magalhaes.gilles.provxlib
package benchmark.configuration

object ExperimentSetup extends Enumeration {
  type ExperimentSetup = Value
  val
  // Combined provenance graph and data graph pruning
  CombinedPruning,
  // Smart data graph pruning
  DataGraphPruning,
  // Only algorithm operation
  ProvenanceGraphPruning,
  // Explore different storage formats (tradeoff compute(=compression)/storage)
  CompleteProvenance,
  // Storage off, compression off
  Tracing,
  // Baseline (no tracing, no capture, no storage)
  Baseline = Value
}
