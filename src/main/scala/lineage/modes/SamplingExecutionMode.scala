package lu.magalhaes.gilles.provxlib
package lineage.modes

import lineage.GraphLineage

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class SamplingExecutionMode(
    fraction: Double,
    withReplacement: Boolean,
    edgesOnly: Boolean = false
) extends ExecutionMode {

  private var frac = fraction

  // TODO: make sampling more powerful
  def setFraction(newFraction: Double): Unit = {
    assert(fraction > 0 && fraction <= 1.0, "Fraction must be between 0 and 1")
    frac = newFraction
  }

  // TODO(gm): generalise this to the entire graph
  // TODO: fix seed
  def filter[VD: ClassTag, ED: ClassTag](
      gl: GraphLineage[VD, ED]
  ): RDD[(VertexId, VD)] = {
    gl.vertices.sample(withReplacement = false, frac)
  }
}
