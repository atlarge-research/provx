package lu.magalhaes.gilles.provxlib
package provenance.storage

import provenance.{GraphLineage, ProvenanceContext}

import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

abstract class StorageHandler {

  def save[V: ClassTag, D: ClassTag](
      spark: SparkSession,
      g: GraphLineage[V, D]
  ): StorageLocation = {
    if (ProvenanceContext.isStorageEnabled) {
      write(spark, g)
    } else {
      EmptyLocation()
    }
  }

  // nameless writes
  def write[V: ClassTag, D: ClassTag](
      spark: SparkSession,
      g: GraphLineage[V, D]
  ): StorageLocation
}
