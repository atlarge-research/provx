package lu.magalhaes.gilles.provxlib
package provenance.storage

import provenance.{GraphLineage, ProvenanceContext}

import scala.reflect.ClassTag

abstract class StorageHandler {

  def save[V: ClassTag, D: ClassTag](g: GraphLineage[V, D]): StorageLocation = {
    if (ProvenanceContext.isStorageEnabled) {
      write(g)
    } else {
      EmptyLocation()
    }
  }

  // nameless writes
  def write[V: ClassTag, D: ClassTag](g: GraphLineage[V, D]): StorageLocation
}
