package lu.magalhaes.gilles.provxlib
package lineage.storage

import lineage.GraphLineage

import org.apache.hadoop.io.compress.GzipCodec

import java.util.UUID
import scala.reflect.ClassTag

trait StorageFormat

case class TextFile(compression: Boolean = false) extends StorageFormat
case class ObjectFile() extends StorageFormat

class HDFSStorageHandler(
    val lineageDirectory: String,
    format: StorageFormat = TextFile()
) extends StorageHandler {

  // nameless writes
  override def save[V: ClassTag, D: ClassTag](
      g: GraphLineage[V, D]
  ): StorageLocation = {
    val name = UUID.randomUUID().toString
    val dir = s"$lineageDirectory/$name.csv"
    println(s"Saving data to ${dir}")
    format match {
      case TextFile(compression) => {
        if (compression) {
          g.vertices.saveAsTextFile(dir, classOf[GzipCodec])
        } else {
          g.vertices.saveAsTextFile(dir)
        }
      }
      case ObjectFile() => g.vertices.saveAsObjectFile(dir)
      case _            => throw new NotImplementedError("unknown storage format")
    }
    HDFSLocation(dir)
  }
}
