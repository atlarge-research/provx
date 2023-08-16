package lu.magalhaes.gilles.provxlib
package lineage.storage
import lineage.GraphLineage

import scala.reflect.ClassTag

class NullStorageHandler extends StorageHandler {
  override def save[V: ClassTag, D: ClassTag](
      g: GraphLineage[V, D]
  ): StorageLocation = {
    println(s"NullStorageDriver: saving ${g.id}")
    EmptyLocation()
  }
}
