package lu.magalhaes.gilles.provxlib
package lib

import lineage.GraphLineage
import utils.{GraphalyticsOutputReader, LocalSparkContext}

import org.apache.spark.SparkContext
import org.scalatest.funsuite.AnyFunSuite

class GraphalyticsAlgorithmsTestSuite extends AnyFunSuite with LocalSparkContext {

  def loadTestGraph(sc: SparkContext, algorithm: String): (GraphLineage[Unit, Double], String) = {
    val expectedOutput = getClass.getResource(s"/example-directed-${algorithm}").toString
    val parent = "/" + expectedOutput.split("/").drop(1).dropRight(1).mkString("/")
    val (graph, _) = Benchmark.loadGraph(sc, parent, "example-directed")
    (new GraphLineage(graph), expectedOutput)
  }

  // OK
  test("BFS") {
    withSpark { sc =>
      val (gl, expectedOutputPath) = loadTestGraph(sc, "BFS")
      val expectedResult = GraphalyticsOutputReader.readFloat(expectedOutputPath)
      val actualResult = gl.bfs(1).getGraph().vertices.collect().sortWith(_._1 < _._1)
      assert(actualResult sameElements expectedResult)
    }
  }

  // OK
  test("SSSP") {
    withSpark { sc =>
      val (gl, expectedOutputPath) = loadTestGraph(sc, "SSSP")
      val expectedResult = GraphalyticsOutputReader.readFloat(expectedOutputPath)
      val actualResult = gl.sssp(1).getGraph().vertices.collect().sortWith(_._1 < _._1)

      assert(expectedResult.length == actualResult.length)

      expectedResult.zip(actualResult).foreach(v => {
        val (expected, actual) = v
        assert(expected._1 == actual._1)
        if (expected._2 == Double.PositiveInfinity || actual._2 == Double.PositiveInfinity) {
          assert(expected._2 == actual._2)
        } else {
          assert(math.abs(expected._2 - actual._2) < 1e-5)
        }
      })
    }
  }

//  test("WCC") {
//    withSpark { sc =>
//      val (graph, _) = Benchmark.loadGraph(
//        sc, "/Users/gm/vu/thesis/impl/provxlib/src/test/resources",
//        "example-directed"
//      )
//      val gl = new GraphLineage(graph)
//      val actualResult = gl.wcc().getGraph().vertices.collect().sortWith(_._1 < _._1)
//      val expectedResult = GraphalyticsOutputReader.readFloat(s"${exampleInputPrefix}-BFS")
//
//      actualResult.foreach(println)
//      println("---")
//      expectedResult.foreach(println)
//
//      assert(actualResult sameElements expectedResult)
//    }
//  }
//
//  test("PageRank") {
//    withSpark { sc =>
//      val (graph, _) = Benchmark.loadGraph(
//        sc, "/Users/gm/vu/thesis/impl/provxlib/src/test/resources",
//        "example-directed"
//      )
////      val results = graph.withLineage().pageRank(numIter = 2)
//      graph.staticPageRank(2).vertices.collect().sortWith(_._1 < _._1).foreach(println)
//
//      // graph.pageRank()results.getGraph().vertices.collect().sortWith(_._1 < _._1).foreach(println)
//    }
//  }

}
