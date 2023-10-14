package lu.magalhaes.gilles.provxlib
package provenance.storage

import provenance.GraphLineage

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession

import java.util.UUID
import scala.reflect.ClassTag

class HDFSStorageHandler(
    val lineageDirectory: String,
    format: StorageFormat = TextFile()
) extends StorageHandler {

  override def write[V: ClassTag, D: ClassTag](
      spark: SparkSession,
      g: GraphLineage[V, D]
  ): StorageLocation = {
    val name = UUID.randomUUID().toString
    val dir = s"$lineageDirectory/$name"
    println(s"Saving data to ${dir}")
    //    val t = g.vertices.map((v) => (v._1, v._2.toString))
    //    val df = spark.createDataFrame(t)
    //    df.write.csv(dir)
    var createDataframe = format match {
      case TextFile(compression) =>
        if (compression) {
          g.vertices.saveAsTextFile(dir + ".txt", classOf[GzipCodec])
        } else {
          g.vertices.saveAsTextFile(dir + ".txt")
        }
        false
      case ObjectFile() =>
        g.vertices.saveAsObjectFile(dir + ".obj")
        false
      case _ => true
    }
    if (createDataframe) {
      val t = g.vertices.map((v) => (v._1, v._2.toString))
      val df = spark.createDataFrame(t)
      val output = df.write
      format match {
        case ParquetFile() =>
          output.format("parquet").save(dir + ".parquet")
        case AvroFile() =>
          output.format("avro").save(dir + ".avro")
        case ORCFile() =>
          output.format("orc").save(dir + ".orc")
        case CSVFile(compression) =>
          if (compression) {
            output.option("compression", "gzip")
          } else {
            output
          }.format("csv").save(dir + ".csv")
        case JSONFormat(compression) =>
          if (compression) {
            output.option("compression", "gzip")
          }.format("json").save(dir + ".json")
      }
    }
    HDFSLocation(dir)
  }
}
