package lu.magalhaes.gilles.provxlib
package benchmark

import benchmark.configuration.BenchmarkConfig

import lu.magalhaes.gilles.provxlib.utils.LocalSparkSession
import lu.magalhaes.gilles.provxlib.utils.LocalSparkSession.withSparkSession
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
          lineageEnabled = true,
          runNr = 1,
          outputDir = outputDir,
          benchmarkConfig = benchmarkConfig,
          lineageDir = benchmarkConfig.lineagePath,
          compressionEnabled = false
        )
      )
      Benchmark.run(sc, config)
    }
  }
}
