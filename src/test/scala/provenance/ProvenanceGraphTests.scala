package lu.magalhaes.gilles.provxlib
package provenance

import utils.LocalSparkContext

import org.apache.spark.graphx.{Edge, Graph}
import org.scalatest.funsuite.AnyFunSuite

class ProvenanceGraphTests extends AnyFunSuite with LocalSparkContext {

  test("Two chained graphs") {
//    withSpark { sc =>
//      assert(true)
//      val g = Graph(sc.parallelize(Seq((1L, ()))), sc.parallelize(Seq(Edge(1, 1, ()))))
//      val gl = new GraphLineage(g)
//
//      val res = gl.bfs(1)
//
//      for ((input, output) <- gl.getProvenanceGraph().allPairs) {
//        println(input.id, output.id)
//      }
//    }
  }
}
