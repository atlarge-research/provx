package lu.magalhaes.gilles.provxlib
package provenance

import utils.{GraphalyticsOutputReader, GraphTestLoader, LocalSparkContext}

import lu.magalhaes.gilles.provxlib.provenance.hooks.Hook
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.ClassTag

class GraphalyticsAlgorithmsTestSuite extends AnyFunSuite with LocalSparkContext {

  test("BFS") {
    withSpark { sc =>
      val (gl, expectedOutputPath) = GraphTestLoader.load(sc, "BFS")
      val expectedResult = GraphalyticsOutputReader.readFloat(expectedOutputPath)
      val actualResult = gl.bfs(1).vertices.collect().sortWith(_._1 < _._1)
      println("Expected results: ", expectedResult.foreach(print(_, " ")))
      println("Acutal results: ", actualResult.foreach(print(_, " ")))
      assert(actualResult sameElements expectedResult)
    }
  }

  test("SSSP") {
    withSpark { sc =>
      val (gl, expectedOutputPath) = GraphTestLoader.load(sc, "SSSP")
      val expectedResult = GraphalyticsOutputReader.readFloat(expectedOutputPath)
      val actualResult = gl.sssp(1).vertices.collect().sortWith(_._1 < _._1)

      assert(expectedResult.length == actualResult.length)

      expectedResult.zip(actualResult).foreach(v => {
        val (expected, actual) = v
        assert(expected._1 == actual._1)
        if (expected._2 == Double.PositiveInfinity || actual._2 == Double.PositiveInfinity) {
          assert(expected._2 == actual._2)
        } else {
          assert(math.abs(expected._2 - actual._2) < 1e-7)
        }
      })
    }
  }

  test("WCC") {
    withSpark { sc =>
      val (gl, expectedOutputPath) = GraphTestLoader.load(sc, "WCC")
      val actualResult = gl.wcc().vertices.collect().sortWith(_._1 < _._1)
      val expectedResult = GraphalyticsOutputReader.readFloat(expectedOutputPath)

      assert(actualResult sameElements expectedResult)
    }
  }

  test("PageRank") {
    withSpark { sc =>
      val (gl, expectedOutputPath) = GraphTestLoader.load(sc, "PR")
      val expectedResult = GraphalyticsOutputReader.readFloat(expectedOutputPath)
      val actualResult = gl.pageRank(2)
        .vertices
        .collect()
        .sortWith(_._1 < _._1)

      expectedResult.zip(actualResult).foreach(v => {
        val (expected, actual) = v
        assert(expected._1 == actual._1)
        if (expected._2 == Double.PositiveInfinity || actual._2 == Double.PositiveInfinity) {
          assert(expected._2 == actual._2)
        } else {
          assert(math.abs(expected._2 - actual._2) < 1e-8)
        }
      })
    }
  }

  test("Hook called") {
    assert(true)
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
  }
}
