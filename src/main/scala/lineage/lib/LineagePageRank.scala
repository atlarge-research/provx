package lu.magalhaes.gilles.provxlib
package lineage.lib

import lineage.{GraphCheckpointer, LineagePregel, PregelIterationMetrics, PregelMetrics}

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object LineagePageRank extends Logging {

  def run[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED],
    numIter: Int,
    dampingFactor: Double = 0.85,
    sampleFraction: Option[Double] = None): (Graph[Double, Unit], PregelMetrics) =
  {
    val vertexCount = graph.numVertices

    var workGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (_, _, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr)
      .mapVertices { (_, _) => 1.0 / vertexCount }
      .cache()

    val danglingVertices = workGraph.vertices.minus(
      workGraph.outDegrees.mapValues(_ => 0.0)
    ).cache()

    val checkpointer = new GraphCheckpointer[Double, Double](graph.vertices.sparkContext, sampleFraction = sampleFraction)
    val metrics = new PregelMetrics(checkpointer.graphLineageDirectory)

    checkpointer.save(workGraph)

    // TODO: add lineage graph to pagerank

    var iteration = 0
    while (iteration < numIter) {
      val prevGraph = workGraph

      val sumOfValues = workGraph.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr),
        _ + _, TripletFields.Src)

      // Compute the sum of all PageRank values of "dangling nodes" in the graph
      val danglingSum = workGraph.vertices.innerJoin(danglingVertices)((_, value, _) => value)
        .aggregate(0.0)((sum, vertexPair) => sum + vertexPair._2, _ + _)

      // Compute the new PageRank value of all nodes
      workGraph = workGraph.outerJoinVertices(sumOfValues)((_, _, newSumOfValues) =>
        (1 - dampingFactor) / vertexCount +
          dampingFactor * (newSumOfValues.getOrElse(0.0) + danglingSum / vertexCount)).cache()


      // Materialise the working graph
      val vertices = workGraph.vertices.count()
      workGraph.edges.count()

      metrics.update(new PregelIterationMetrics(vertices))

      checkpointer.save(workGraph)

      // Unpersist the previous cached graph
      prevGraph.unpersist(false)

      iteration += 1
    }

    (workGraph.mapEdges(_ => Unit), metrics)
  }
}
