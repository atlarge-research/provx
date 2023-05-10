package lu.magalhaes.gilles.provxlib
package lib

import lineage.GraphLineage.graphToGraphLineage
import utils.LocalSparkContext

import org.apache.spark.graphx.Graph
import org.scalatest.funsuite.AnyFunSuite

class LineagePageRankTestSuite extends AnyFunSuite with LocalSparkContext {
  test("hello world") {
    withSpark { sc =>
      val (graph, _) = Benchmark.loadGraph(
        sc, "/Users/gm/vu/thesis/impl/provxlib/src/test/resources",
        "example-directed"
      )
//      val results = graph.withLineage().pageRank(numIter = 2)
      graph.staticPageRank(2).vertices.collect().sortWith(_._1 < _._1).foreach(println)
      // graph.pageRank()results.getGraph().vertices.collect().sortWith(_._1 < _._1).foreach(println)
    }
  }
}
