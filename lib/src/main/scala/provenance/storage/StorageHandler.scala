package lu.magalhaes.gilles.provxlib
package provenance.storage

import provenance.{GraphLineage, ProvenanceContext}
import provenance.hooks.TimeHook

import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

abstract class StorageHandler {

  def save[V: ClassTag, D: ClassTag](
      spark: SparkSession,
      g: GraphLineage[V, D]
  ): StorageLocation = if (ProvenanceContext.storageEnabled) {
    val th = TimeHook("storageTime")
    th.pre(g)
    val res = write(spark, g)
    th.post(g)
    res
  } else {
    EmptyLocation()
  }

  // nameless writes
  def write[V: ClassTag, D: ClassTag](
      spark: SparkSession,
      g: GraphLineage[V, D]
  ): StorageLocation
}
