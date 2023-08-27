package lu.magalhaes.gilles.provxlib
package provenance.events

import org.apache.spark.graphx.VertexId

trait EventType

trait Algorithm extends EventType

case class BFS(source: Long) extends Algorithm
//case class Algorithm(name: String) extends EventType

case class PageRank(numIter: Int, dampingFactor: Double = 0.85)
    extends Algorithm
case class WCC(maxIterations: Int = Int.MaxValue) extends Algorithm

case class SSSP(source: VertexId) extends Algorithm

case class Operation(name: String) extends EventType
case class PregelAlgorithm() extends EventType

case class PregelLifecycleStart() extends EventType
case class PregelLifecycleStop() extends EventType
case class PregelIteration(iteration: Long) extends EventType
