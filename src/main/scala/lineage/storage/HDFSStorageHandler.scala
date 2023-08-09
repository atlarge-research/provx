package lu.magalhaes.gilles.provxlib
package lineage.storage

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class HDFSStorageHandler() extends StorageHandler {

    private var lineageDir: Option[String] = None

    override def save[V: ClassTag, D: ClassTag](g: Graph[V, D]): Location = {
      // todo: generate path and return it
      val path = "asdf"
      println(s"Saving data to ${path}")
      g.vertices.saveAsTextFile(path)
      Location()
    }

    def getLineageDir: Option[String] = lineageDir

//    def setLineageDir(directory: String): Unit = {
//      val dir = new Path(directory)
//      val fs = dir.getFileSystem(hadoopConfiguration)
//      println(fs.exists(dir), s"${directory} does not exist.")
//      lineageDir = Some(fs.getFileStatus(dir).getPath.toString)
//    }
//
//    def createNewLineageDirectory(): String = {
//      // TODO: needs to be based on something to be unique and "recognizable"
//      val uniqueDirectory = UUID.randomUUID().toString
//      val path = new Path(getLineageDir.get, uniqueDirectory)
//      val fs = path.getFileSystem(hadoopConfiguration)
//      fs.mkdirs(path)
//      fs.getFileStatus(path).getPath.toString
//    }
}
