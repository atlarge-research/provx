package lu.magalhaes.gilles.provxlib
package lineage.storage

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

trait StorageHandler {

  def getLineageDir: Option[String]

  def setLineageDir(directory: String): Unit

  def createNewLineageDirectory(): String
  def save[V: ClassTag, D: ClassTag] (g: Graph[V, D], path: String): Unit
}
