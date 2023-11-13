package lu.magalhaes.gilles.provxlib
package provenance.storage

import provenance.GraphLineage

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession

import java.util.UUID
import scala.reflect.ClassTag

class HDFSStorageHandler(
    val lineageDirectory: String,
    format: StorageFormat
) extends StorageHandler {

  override def write[V: ClassTag, D: ClassTag](
      spark: SparkSession,
      g: GraphLineage[V, D]
  ): StorageLocation = {
    val name = UUID.randomUUID().toString
    val path = s"$lineageDirectory/$name.${StorageFormat.extension(format)}"
    println(s"Saving data to ${path} in format ${format}")
    val createDataframe = format match {
      case TextFile(compression) =>
        if (compression) {
          g.vertices.saveAsTextFile(path, classOf[GzipCodec])
        } else {
          g.vertices.saveAsTextFile(path)
        }
        false
      case ObjectFile() =>
        g.vertices.saveAsObjectFile(path)
        false
      case _ => true
    }
    if (createDataframe) {
      val t = g.vertices.map(v => (v._1, v._2.toString))
      val df = spark.createDataFrame(t)
      val output = format match {
        case CSVFile(true) | JSONFormat(true) =>
          df.write.option("compression", "gzip")
        case _ => df.write
      }

      output.format(StorageFormat.extension(format)).save(path)
    }
    HDFSLocation(path)
  }
}
