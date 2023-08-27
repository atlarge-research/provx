package lu.magalhaes.gilles.provxlib
package provenance.storage

import provenance.GraphLineage

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession

import java.util.UUID
import scala.reflect.ClassTag

trait StorageFormat

case class TextFile(compression: Boolean = false) extends StorageFormat
case class ObjectFile() extends StorageFormat

class HDFSStorageHandler(
    val lineageDirectory: String,
    format: StorageFormat = TextFile()
) extends StorageHandler {

  override def write[V: ClassTag, D: ClassTag](
      spark: SparkSession,
      g: GraphLineage[V, D]
  ): StorageLocation = {
    val name = UUID.randomUUID().toString
    val dir = s"$lineageDirectory/$name.csv"
    println(s"Saving data to ${dir}")
//    val t = g.vertices.map((v) => (v._1, v._2.toString))
//    val df = spark.createDataFrame(t)
//    df.write.csv(dir)
    format match {
      case TextFile(compression) =>
        if (compression) {
          g.vertices.saveAsTextFile(dir, classOf[GzipCodec])
        } else {
          g.vertices.saveAsTextFile(dir)
        }
      case ObjectFile() => g.vertices.saveAsObjectFile(dir)
      case _            => throw new NotImplementedError("unknown storage format")
    }
    HDFSLocation(dir)
  }
}
