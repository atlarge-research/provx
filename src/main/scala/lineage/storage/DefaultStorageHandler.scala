package lu.magalhaes.gilles.provxlib
package lineage.storage
import lineage.LineageLocalContext

import org.apache.hadoop.fs.Path
import org.apache.spark.graphx.Graph

import java.util.UUID
import scala.reflect.ClassTag

class DefaultStorageHandler(val lineageContext: LineageLocalContext) extends StorageHandler {

  private var lineageDir: Option[String] = None

  override def save[V: ClassTag, D: ClassTag](g: Graph[V, D], path: String): Unit = {
    g.vertices.saveAsTextFile(path)
  }

  override def getLineageDir: Option[String] = lineageDir

  override def setLineageDir(directory: String): Unit = {
    val dir = new Path(directory)
    val fs = dir.getFileSystem(lineageContext.sparkContext.hadoopConfiguration)
    println(fs.exists(dir), s"${directory} does not exist.")
    lineageDir = Some(fs.getFileStatus(dir).getPath.toString)
    lineageContext.enableTracing()
  }

  override def createNewLineageDirectory(): String = {
    // TODO: needs to be based on something to be unique and "recognizable"
    val uniqueDirectory = UUID.randomUUID().toString
    val path = new Path(getLineageDir.get, uniqueDirectory)
    val fs = path.getFileSystem(lineageContext.sparkContext.hadoopConfiguration)
    fs.mkdirs(path)
    fs.getFileStatus(path).getPath.toString
  }
}
