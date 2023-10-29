package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.{
  BenchmarkAppConfig,
  ExperimentSetup,
  GraphAlgorithm,
  RunnerConfig
}
import provenance.{ProvenanceGraph, ProvenanceGraphNode}
import provenance.events.{BFS, Operation}
import provenance.metrics.ObservationSet

import lu.magalhaes.gilles.provxlib.provenance.storage.TextFile
import lu.magalhaes.gilles.provxlib.utils.LocalSparkSession.withSparkSession
import org.apache.spark.graphx.{Edge, Graph}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Paths

class BenchmarkTests extends AnyFunSuite {
  test("Benchmark test") {
    withSparkSession { sc =>
      val runnerConfig =
        RunnerConfig.loadResource("runner-config-example.conf") match {
          case Left(errors) =>
            fail(s"Could not load configuration: ${errors}")
          case Right(value) => value
        }

      val dataset = "example-directed"
      val outputDir = os.Path("/tmp/test")

      os.remove.all(outputDir)
      os.remove.all(
        os.Path(runnerConfig.runner.outputPath.stripPrefix("file://"))
      )

      val graphalyticsConfigPath = Paths
        .get(
          getClass.getClassLoader
            .getResource("example-directed.properties")
            .toURI
        )
        .toFile
        .getAbsolutePath

      val datasetPath = graphalyticsConfigPath
        .split("/")
        .dropRight(1)
        .mkString("/")

      val config = Benchmark.Config(
        BenchmarkAppConfig(
          experimentID = "1",
          dataset = dataset,
          datasetPath = datasetPath,
          algorithm = GraphAlgorithm.BFS,
          runNr = 1,
          storageFormat = TextFile(),
          outputDir = outputDir,
          graphalyticsConfigPath = graphalyticsConfigPath,
          lineageDir = runnerConfig.runner.lineagePath,
          setup = ExperimentSetup.Baseline
        )
      )
      Benchmark.run(sc, config)
    }
  }

  test("Benchmark flags computation") {
    assert(
      Benchmark.computeFlags(ExperimentSetup.Compression) == (true, true)
    )
    assert(
      Benchmark.computeFlags(ExperimentSetup.Storage) == (true, true)
    )
    assert(
      Benchmark.computeFlags(ExperimentSetup.Tracing) == (true, false)
    )
    assert(
      Benchmark.computeFlags(
        ExperimentSetup.SmartPruning
      ) == (true, true)
    )
    assert(
      Benchmark.computeFlags(
        ExperimentSetup.AlgorithmOpOnly
      ) == (true, true)
    )
    assert(
      Benchmark.computeFlags(
        ExperimentSetup.JoinVerticesOpOnly
      ) == (true, true)
    )
    assert(
      Benchmark.computeFlags(
        ExperimentSetup.Combined
      ) == (true, true)
    )
    assert(
      Benchmark.computeFlags(ExperimentSetup.Baseline) == (false, false)
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
          Benchmark
            .dataFilter(ExperimentSetup.SmartPruning, GraphAlgorithm.WCC)
        ).vertices
          .collect()
          .length == 1
      )

      assert(
        g.subgraph(vpred =
          Benchmark.dataFilter(ExperimentSetup.Baseline, GraphAlgorithm.WCC)
        ).vertices
          .collect()
          .length == 3
      )

      val g2 = Graph(doubleVertices, edges)
      assert(
        g2.subgraph(vpred =
          Benchmark
            .dataFilter(ExperimentSetup.SmartPruning, GraphAlgorithm.SSSP)
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
