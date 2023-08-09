package lu.magalhaes.gilles.provxlib
package lineage.storage

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

trait StorageHandler {

  def save[V: ClassTag, D: ClassTag] (g: Graph[V, D]): Location
}
