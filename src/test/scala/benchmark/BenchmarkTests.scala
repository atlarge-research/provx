package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.BenchmarkConfig

import lu.magalhaes.gilles.provxlib.benchmark.ExperimentParameters.{SSSP, WCC}
import lu.magalhaes.gilles.provxlib.provenance.{
  ProvenanceGraph,
  ProvenanceGraphNode
}
import lu.magalhaes.gilles.provxlib.provenance.events.{BFS, Operation}
import lu.magalhaes.gilles.provxlib.provenance.metrics.ObservationSet
import lu.magalhaes.gilles.provxlib.utils.LocalSparkSession.withSparkSession
import org.apache.spark.graphx.{Edge, Graph}
import org.scalatest.funsuite.AnyFunSuite

class BenchmarkTests extends AnyFunSuite {
  test("Benchmark test") {
    withSparkSession { sc =>
      val configPath =
        getClass.getResource(s"/example-config.properties").toString
      //    val expectedOutput = getClass.getResource(s"/example-directed").toString
      val benchmarkConfig = new BenchmarkConfig(configPath)
      val outputDir = os.Path("/tmp/test")
      os.remove.all(outputDir)
      os.remove.all(os.Path(benchmarkConfig.outputPath.stripPrefix("file://")))
      val config = Benchmark.Config(
        description = ExperimentDescription(
          experimentID = "1",
          dataset = "example-directed",
          algorithm = ExperimentParameters.BFS(),
          setup = ExperimentSetup.Storage.toString,
          runNr = 1,
          outputDir = outputDir,
          benchmarkConfig = benchmarkConfig,
          lineageDir = benchmarkConfig.lineagePath
        )
      )
      Benchmark.run(sc, config)
    }
  }

  test("Benchmark flags computation") {
    assert(
      Benchmark.computeFlags(ExperimentSetup.Compression) == (true, true, true)
    )
    assert(
      Benchmark.computeFlags(ExperimentSetup.Storage) == (true, true, false)
    )
    assert(
      Benchmark.computeFlags(ExperimentSetup.Tracing) == (true, false, false)
    )
    assert(
      Benchmark.computeFlags(
        ExperimentSetup.SmartPruning
      ) == (true, true, false)
    )
    assert(
      Benchmark.computeFlags(
        ExperimentSetup.AlgorithmOpOnly
      ) == (true, true, false)
    )
    assert(
      Benchmark.computeFlags(
        ExperimentSetup.JoinVerticesOpOnly
      ) == (true, true, false)
    )
    assert(
      Benchmark.computeFlags(
        ExperimentSetup.Combined
      ) == (true, true, true)
    )
    assert(
      Benchmark.computeFlags(ExperimentSetup.Baseline) == (false, false, false)
    )
  }

  test("Benchmark data filter") {
    withSparkSession { sc =>
      val longVertices = sc.sparkContext.parallelize(
        Seq(
          (1L, 1L),
          (2L, Long.MaxValue),
          (3L, Long.MaxValue)
        )
      )

      val doubleVertices = sc.sparkContext.parallelize(
        Seq(
          (1L, 1.0),
          (2L, Double.PositiveInfinity),
          (3L, Double.PositiveInfinity)
        )
      )

      val edges = sc.sparkContext.parallelize(
        Seq(
          Edge(1L, 2L, 0.2),
          Edge(2L, 3L, 0.2),
          Edge(3L, 1L, 0.2)
        )
      )

      val g = Graph(longVertices, edges)

      assert(
        g.subgraph(vpred =
          Benchmark.dataFilter(ExperimentSetup.SmartPruning, WCC())
        ).vertices
          .collect()
          .length == 1
      )

      assert(
        g.subgraph(vpred =
          Benchmark.dataFilter(ExperimentSetup.Baseline, WCC())
        ).vertices
          .collect()
          .length == 3
      )

      val g2 = Graph(doubleVertices, edges)
      assert(
        g2.subgraph(vpred =
          Benchmark.dataFilter(ExperimentSetup.SmartPruning, SSSP())
        ).vertices
          .collect()
          .length == 1
      )
    }
  }

  test("Benchmark provenance graph filter") {
    class DummyProvenanceGraphNode(val id: Int) extends ProvenanceGraphNode {}

    val pg = new ProvenanceGraph()
    pg.add(
      new DummyProvenanceGraphNode(1),
      new DummyProvenanceGraphNode(2),
      ProvenanceGraph.Edge(Operation("joinVertices"), ObservationSet())
    )
    pg.add(
      new DummyProvenanceGraphNode(2),
      new DummyProvenanceGraphNode(3),
      ProvenanceGraph.Edge(Operation("mapVertices"), ObservationSet())
    )
    pg.add(
      new DummyProvenanceGraphNode(3),
      new DummyProvenanceGraphNode(4),
      ProvenanceGraph.Edge(Operation("joinVertices"), ObservationSet())
    )
    pg.add(
      new DummyProvenanceGraphNode(1),
      new DummyProvenanceGraphNode(4),
      ProvenanceGraph.Edge(BFS(3), ObservationSet())
    )

    val algOpFilter =
      Benchmark.provenanceFilter(ExperimentSetup.AlgorithmOpOnly)

    val res = pg.filter(nodeP = ProvenanceGraph.allNodes, edgeP = algOpFilter)

    assert(res.graph.edges.count((e: ProvenanceGraph.Type#EdgeT) => {
      algOpFilter(e.outer)
    }) == 1)

    val joinVerticesFilter =
      Benchmark.provenanceFilter(ExperimentSetup.JoinVerticesOpOnly)

    val res2 =
      pg.filter(nodeP = ProvenanceGraph.allNodes, edgeP = joinVerticesFilter)

    assert(res2.graph.edges.count((e: ProvenanceGraph.Type#EdgeT) => {
      joinVerticesFilter(e.outer)
    }) == 2)
  }
}
