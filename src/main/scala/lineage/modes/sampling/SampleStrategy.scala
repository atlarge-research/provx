package lu.magalhaes.gilles.provxlib
package lineage.modes.sampling

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

trait SampleStrategy {

  def sample[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[VD, ED]

}
