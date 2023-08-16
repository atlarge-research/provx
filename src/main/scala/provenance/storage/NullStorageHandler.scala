package lu.magalhaes.gilles.provxlib
package provenance.storage
import provenance.GraphLineage

import scala.reflect.ClassTag

class NullStorageHandler extends StorageHandler {
  override def write[V: ClassTag, D: ClassTag](
      g: GraphLineage[V, D]
  ): StorageLocation = {
    println(s"NullStorageDriver: saving ${g.id}")
    EmptyLocation()
  }
}
