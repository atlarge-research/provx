package lu.magalhaes.gilles.provxlib
package lineage.modes

import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class SamplingMode(fraction: Double, withReplacement: Boolean) extends Mode {

  private var frac = fraction

  // TODO: make sampling more powerful
  def setFraction(newFraction: Double): Unit = {
    assert(fraction > 0 && fraction <= 1.0, "Fraction must be between 0 and 1")
    frac = newFraction
  }

  // TODO(gm): generalise this to the entire graph
  def filter[VD: ClassTag](gl: VertexRDD[VD]): RDD[(VertexId, VD)] = {
      gl.sample(withReplacement = false, frac)
  }
}
