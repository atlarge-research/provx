package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.{GraphalyticsConfig, RunnerConfig}

import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class ConfigFilesTest extends AnyFunSuite {
  test("Read Graphalytics config") {
    val source = GraphalyticsConfig
      .loadResource("example-directed.properties") match {
      case Left(errors) =>
        println(s"Failed to load configuration: $errors")
        fail()
      case Right(config) => config
    }

    assert(source.algorithms.length == 6)
    assert(source.bfs.get.sourceVertex == 1)
    assert(source.pr.get.numIterations == 2)
  }

  test("Read runner config") {
    val runnerConfig =
      RunnerConfig.loadResource("runner-config-example.properties") match {
        case Left(errors) =>
          fail(s"Could not load configuration: ${errors}")
        case Right(value) => value
      }

    println(runnerConfig.runner.algorithms)

    assert(runnerConfig.runner.jar == "invalid-path")
  }
}
