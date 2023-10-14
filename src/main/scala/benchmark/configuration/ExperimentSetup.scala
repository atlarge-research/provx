package lu.magalhaes.gilles.provxlib
package benchmark.configuration

object ExperimentSetup extends Enumeration {
  type ExperimentSetup = Value
  val
  // Explore different storage formats (tradeoff compute(=compression)/storage)
  StorageFormats,
  // Storage on, compression on
  Compression,
  // Storage on, compression off
  Storage,
  // Storage off, compression off
  Tracing,
  // Smart data graph pruning
  SmartPruning,
  // Only algorithm operation
  AlgorithmOpOnly,
  // Only joinVertices operation
  JoinVerticesOpOnly,
  // Combined joinVertices, smart-pruning and compression
  Combined,
  // Baseline (no tracing, no storage, nothing)
  Baseline = Value
}
