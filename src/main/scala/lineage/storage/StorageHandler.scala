package lu.magalhaes.gilles.provxlib
package lineage.storage

import lineage.{GraphLineage, LineageContext}

import scala.reflect.ClassTag

abstract class StorageHandler {

  def save[V: ClassTag, D: ClassTag](g: GraphLineage[V, D]): StorageLocation = {
    if (LineageContext.isStorageEnabled) {
      write(g)
    } else {
      EmptyLocation()
    }
  }

  // nameless writes
  def write[V: ClassTag, D: ClassTag](g: GraphLineage[V, D]): StorageLocation
}
