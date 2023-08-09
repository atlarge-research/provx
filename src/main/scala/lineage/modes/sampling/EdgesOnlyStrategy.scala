package lu.magalhaes.gilles.provxlib
package lineage.modes.sampling

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class EdgesOnlyStrategy extends SampleStrategy {
  override def sample[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VD, ED] = {
    // TODO: determine how sampling should be done
    // When sampling edges, should we also take into consideration the nodes
    // and keep one single VertexRDD representing those, so that the graph is
    // complete?
    graph
  }
}
