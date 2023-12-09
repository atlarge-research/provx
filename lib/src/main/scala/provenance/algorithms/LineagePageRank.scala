package lu.magalhaes.gilles.provxlib
package provenance.algorithms

import provenance.{GraphLineage, Utils}
import provenance.events.{PregelIteration, PregelLifecycleStart, PregelLifecycleStop}
import provenance.metrics.{Gauge, ObservationSet}

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object LineagePageRank extends Logging {

  def run[VD: ClassTag, ED: ClassTag](
      gl: GraphLineage[VD, ED],
      numIter: Int,
      dampingFactor: Double = 0.85
  ): GraphLineage[Double, Unit] = {
    val vertexCount = gl.numVertices

    var workGraph: GraphLineage[Double, Double] =
      Utils.trace(gl, PregelLifecycleStart()) {
        gl
          // Associate the degree with each vertex
          .outerJoinVertices(gl.outDegrees) { (_, _, deg) =>
            deg.getOrElse(0)
          }
          // Set the weight on the edges based on the degree
          .mapTriplets(e => 1.0 / e.srcAttr)
          .mapVertices { (_, _) => 1.0 / vertexCount }
          .cache()
      }

    val danglingVertices = workGraph.vertices
      .minus(
        workGraph.outDegrees.mapValues(_ => 0.0)
      )
      .cache()

    val metrics = ObservationSet()

    var iteration = 0
    while (iteration < numIter) {
      val prevGraph = workGraph

      val sumOfValues = workGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr),
        _ + _,
        TripletFields.Src
      )

      // Compute the sum of all PageRank values of "dangling nodes" in the graph
      val danglingSum = workGraph.vertices
        .innerJoin(danglingVertices)((_, value, _) => value)
        .aggregate(0.0)((sum, vertexPair) => sum + vertexPair._2, _ + _)

      // Compute the new PageRank value of all nodes
      workGraph = Utils.trace(workGraph, PregelIteration(iteration)) {
        workGraph
          .outerJoinVertices(sumOfValues)((_, _, newSumOfValues) =>
            (1 - dampingFactor) / vertexCount +
              dampingFactor * (newSumOfValues
                .getOrElse(0.0) + danglingSum / vertexCount)
          )
          .cache()
      }

      // Materialise the working graph
      val vertices = workGraph.vertices.count()
      workGraph.edges.count()

      metrics.add(Gauge("numVertices", vertices))

      // Unpersist the previous cached graph
      prevGraph.unpersist()

      iteration += 1
    }

    Utils.trace(workGraph, PregelLifecycleStop()) {
      val newGl = workGraph.mapEdges(_ => ())
      newGl.metrics.merge(metrics)
      newGl
    }
  }
}
