package lu.magalhaes.gilles.provxlib
package lineage.modes.sampling

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class VertexOnlyStrategy extends SampleStrategy {
  override def sample[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VD, ED] = {
    // TODO: determine how sampling should be done
    // When sampling vertices, should we also take into consideration the edges
    // that connect the sampled vertices and only keep one single EdgeRDD around?
    graph
  }
}
