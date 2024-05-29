package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration._
import provenance.storage.TextFile

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Paths

class ConfigFilesTest extends AnyFunSuite {
  test("Parse Graphalytics configurations successfully") {
    val configurations = Seq(
      "cit-Patents.properties",
      "datagen-7_5-fb.properties",
      "datagen-7_6-fb.properties",
      "datagen-7_7-zf.properties",
      "datagen-7_8-zf.properties",
      "datagen-7_9-fb.properties",
      "datagen-8_4-fb.properties",
      "datagen-8_8-zf.properties",
      "dota-league.properties",
      "graph500-22.properties",
      "kgs.properties",
      "wiki-Talk.properties"
    )

    for (configuration <- configurations) {
      GraphalyticsConfig
        .loadResource(s"graphalytics/${configuration}") match {
        case Left(errors) =>
          println(s"Failed to load configuration: $errors")
          fail()
        case Right(config) => config
      }
    }
  }

  test("Read Graphalytics config") {
    val source = GraphalyticsConfig
      .loadResource("example-directed.properties") match {
      case Left(errors) =>
        println(s"Failed to load configuration: $errors")
        fail()
      case Right(config) => config
    }

    assert(source.algorithms.toList.length == 6)
    assert(source.bfs.get.sourceVertex == 1)
    assert(source.pr.get.numIterations == 2)
  }

  test("Read runner config") {
    val runnerConfig =
      RunnerConfig.loadResource("runner-config-example.conf") match {
        case Left(errors) =>
          fail(s"Could not load configuration: ${errors}")
        case Right(value) => value
      }

    println(RunnerConfig.write(runnerConfig))
    println(runnerConfig.runner.algorithms)
    println(runnerConfig.runner.graphs)
    println(runnerConfig.runner.setups)

    assert(runnerConfig.runner.jar == "invalid-path")
  }

  test("Test BenchmarkAppConfig") {
    val runnerConfig =
      RunnerConfig.loadResource("runner-config-example.conf") match {
        case Left(errors) =>
          fail(s"Could not load configuration: ${errors}")
        case Right(value) => value
      }

    val dataset = "example-directed"
    val outputDir = os.Path("/tmp/test")

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

    val config = BenchmarkAppConfig(
      experimentID = "1",
      dataset = dataset,
      datasetPath = datasetPath,
      algorithm = GraphAlgorithm.BFS,
      runNr = 1,
      storageFormat = TextFile(),
      outputDir = outputDir,
      graphalyticsConfigPath = graphalyticsConfigPath,
      lineageDir = runnerConfig.runner.lineagePath,
      setup = ExperimentSetup.Baseline,
      numExecutors = 7
    )

    println(BenchmarkAppConfig.write(config))
  }
}
