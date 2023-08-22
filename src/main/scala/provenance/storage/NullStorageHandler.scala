package lu.magalhaes.gilles.provxlib
package provenance.storage
import provenance.GraphLineage

import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class NullStorageHandler extends StorageHandler {
  override def write[V: ClassTag, D: ClassTag](
      spark: SparkSession,
      g: GraphLineage[V, D]
  ): StorageLocation = {
    println(s"NullStorageDriver: saving ${g.id}")
    EmptyLocation()
  }
}
