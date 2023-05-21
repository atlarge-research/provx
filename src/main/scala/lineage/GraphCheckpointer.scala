package lu.magalhaes.gilles.provxlib
package lineage

import org.apache.hadoop.fs.Path
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.SparkContext

import java.util.UUID
import scala.reflect.ClassTag

class GraphCheckpointer[VD: ClassTag, ED: ClassTag](
   sparkContext: SparkContext,
   pruneLineage: Option[((VertexId, VD)) => Boolean] = None,
   sampleFraction: Option[Double] = None) {

  private var iteration = 0

  private val uniqueDirectory = UUID.randomUUID().toString

  def graphLineageDirectory: String = uniqueDirectory

  private def createDirectoryIfRequired(): String = {
    val path = new Path(LineageContext.getLineageDir.get, uniqueDirectory)
    val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
    fs.mkdirs(path)
    fs.getFileStatus(path).getPath.toString
  }

  def save(g: Graph[VD, ED]): Unit = {
    if (!LineageContext.isCheckpointingEnabled) {
      return
    }

    require(
      LineageContext.isCheckpointingEnabled && LineageContext.getLineageDir.isDefined,
      "No lineage directory defined. Use LineageContext.setLineageDir(dir) to save lineage" +
        " output."
    )

    val lineagePath = createDirectoryIfRequired()

    val prunedVertices = if (pruneLineage.isDefined) {
      g.vertices.filter(pruneLineage.get)
    } else {
      g.vertices
    }

    val vertices = if (sampleFraction.isDefined) {
      prunedVertices.sample(withReplacement = false, sampleFraction.get)
    } else {
      prunedVertices
    }

    if (LineageContext.isCheckpointingEnabled) {
      val path = f"${lineagePath}/${iteration}%04d.v"
      println(s"Saving data to ${path}")
      vertices.saveAsTextFile(path)
      iteration += 1
    }
  }
}
