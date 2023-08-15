package lu.magalhaes.gilles.provxlib
package lineage.storage
import lu.magalhaes.gilles.provxlib.lineage.GraphLineage
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class NullStorageHandler extends StorageHandler {
  override def save[V: ClassTag, D: ClassTag](g: GraphLineage[V, D]): StorageLocation = {
    println(s"NullStorageDriver: saving ${g.id}")
    EmptyLocation()
  }
}
