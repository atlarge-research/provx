package lu.magalhaes.gilles.provxlib
package lineage.storage

import lineage.GraphLineage

import scala.reflect.ClassTag

trait StorageHandler {

  def save[V: ClassTag, D: ClassTag](g: GraphLineage[V, D]): StorageLocation
}
