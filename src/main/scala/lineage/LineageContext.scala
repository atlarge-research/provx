package lu.magalhaes.gilles.provxlib
package lineage

import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext

object LineageContext {

  private var lineageDir: Option[String] = None

  private var checkpointingEnabled = false

  def setLineageDir(sparkContext: SparkContext, directory: String): Unit = {
    lineageDir = Option(directory).map { dir =>
      val path = new Path(dir, UUID.randomUUID().toString)
      val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
      fs.mkdirs(path)
      fs.getFileStatus(path).getPath.toString
    }
    checkpointingEnabled = true
  }

  def disableCheckpointing(): Unit = {
    checkpointingEnabled = false
  }

  def isCheckpointingEnabled: Boolean = {
    checkpointingEnabled
  }

  def getLineageDir: Option[String] = lineageDir
}
