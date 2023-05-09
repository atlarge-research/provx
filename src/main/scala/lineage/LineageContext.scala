package lu.magalhaes.gilles.provxlib
package lineage

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

object LineageContext {

  private var lineageDir: Option[String] = None

  private var checkpointingEnabled = false

  def setLineageDir(sparkContext: SparkContext, directory: String): Unit = {
    val dir = new Path(directory)
    val fs = dir.getFileSystem(sparkContext.hadoopConfiguration)
    println(fs.exists(dir), s"${directory} does not exist.")
    lineageDir = Some(fs.getFileStatus(dir).getPath.toString)
    checkpointingEnabled = true
  }

  def enableCheckpointing(): Unit = {
    require(lineageDir.isDefined, "Lineage directory is not defined.")
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
