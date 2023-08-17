package lu.magalhaes.gilles.provxlib
package provenance

import utils.{GraphalyticsOutputReader, GraphTestLoader, LocalSparkSession}

import lu.magalhaes.gilles.provxlib.utils.LocalSparkSession.withSparkSession
import org.apache.spark.graphx.VertexId
import org.apache.spark.SparkContext
import org.scalatest.funsuite.AnyFunSuite

class GraphalyticsAlgorithmsTestSuite
    extends AnyFunSuite
    with LocalSparkSession {

  def algorithmTestLong(
      sc: SparkContext,
      algorithm: String
  ): (Array[(VertexId, Long)], Array[(VertexId, Long)]) = {
    val (gl, expectedOutputPath) =
      GraphTestLoader.load(sc, algorithm)
    val expectedResult =
      GraphalyticsOutputReader.readLong(expectedOutputPath)

    val actualResult = algorithm match {
      case "BFS" => gl.bfs(1)
      case "WCC" => gl.wcc()
      case _     => throw new NotImplementedError("unknown graph algorithm")
    }

    val vertices = actualResult.vertices.collect().sortWith(_._1 < _._1)

    (vertices, expectedResult)
  }

  def algorithmTestDouble(
      sc: SparkContext,
      algorithm: String
  ): (Array[(VertexId, Double)], Array[(VertexId, Double)]) = {
    val (gl, expectedOutputPath) =
      GraphTestLoader.load(sc, algorithm)
    val expectedResult =
      GraphalyticsOutputReader.readDouble(expectedOutputPath)

    val actualResult = algorithm match {
      case "SSSP" => gl.sssp(1)
      case "PR"   => gl.pageRank(2)
      case _      => throw new NotImplementedError("unknown graph algorithm")
    }

    val vertices = actualResult.vertices.collect().sortWith(_._1 < _._1)

    (vertices, expectedResult)
  }

  def assertVerticesEqualDouble(
      v1: Array[(VertexId, Double)],
      v2: Array[(VertexId, Double)]
  ): Unit = {
    assert(v1.length == v2.length)
    v1
      .zip(v2)
      .foreach(v => {
        val (expected, actual) = v

        // IDs must be the same otherwise v1 or v2 is not sorted
        assert(expected._1 == actual._1)
        if (
          expected._2 == Double.PositiveInfinity || actual._2 == Double.PositiveInfinity
        ) {
          assert(expected._2 == actual._2)
        } else {
          assert(math.abs(expected._2 - actual._2) < 1e-7)
        }
      })
  }

  def assertVerticesEqualLong(
      v1: Array[(VertexId, Long)],
      v2: Array[(VertexId, Long)]
  ): Unit = {
    assert(v1.length == v2.length)
    v1
      .zip(v2)
      .foreach(v => {
        val (expected, actual) = v
        // IDs must be the same otherwise v1 or v2 is not sorted
        assert(expected._1 == actual._1)
        assert(expected._2 == actual._2)
      })
  }

  test("BFS") {
    withSparkSession(spark) { sc =>
      val (actualResult, expectedResult) =
        algorithmTestLong(sc.sparkContext, "BFS")
      assertVerticesEqualLong(actualResult, expectedResult)
    }
  }

  test("SSSP") {
    withSparkSession(spark) { sc =>
      val (actualResult, expectedResult) =
        algorithmTestDouble(sc.sparkContext, "SSSP")
      assertVerticesEqualDouble(actualResult, expectedResult)
    }
  }

  test("WCC") {
    withSparkSession(spark) { sc =>
      val (actualResult, expectedResult) =
        algorithmTestLong(sc.sparkContext, "WCC")
      assertVerticesEqualLong(actualResult, expectedResult)
    }
  }

  test("PageRank") {
    withSparkSession(spark) { sc =>
      val (actualResult, expectedResult) =
        algorithmTestDouble(sc.sparkContext, "PR")

      assertVerticesEqualDouble(actualResult, expectedResult)
    }
  }

//  test("Hook called") {
//    assert(true)
//    withSpark { sc =>
//      class CalledHook(f: () => Unit) extends Hook {
//
//        override def shouldInvoke(eventName: ProvenanceGraph.Relation): Boolean = eventName == "pageRank"
//
//        override def post[VD: ClassTag, ED: ClassTag](outputGraph: GraphLineage[VD, ED]): Unit = {
//          f()
//        }
//      }
//
//      var hookCalled = false
//
//      def called(): Unit = {
//        hookCalled = true
//      }
//
//      LineageContext.hooks.register(new CalledHook(called))
//
//      val (gl, _) = GraphTestLoader.load(sc, "PR")
//      gl.pageRank(1)
//
//      assert(hookCalled, "The hook was not called.")
//    }
//  }
}
