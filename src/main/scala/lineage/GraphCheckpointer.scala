package lu.magalhaes.gilles.provxlib
package lineage

import lu.magalhaes.gilles.provxlib.lineage.modes.{BatchMode, InteractiveMode, SamplingMode}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class GraphCheckpointer[VD: ClassTag, ED: ClassTag](lineageContext: LineageLocalContext) {

  private var iteration = 0

//  private val uniqueDirectory = UUID.randomUUID().toString

  def save(g: Graph[VD, ED]): Unit = {
    if (!lineageContext.isTracingEnabled) {
      return
    }

    val storageHandler = lineageContext.getStorageHandler

    require(
      lineageContext.isTracingEnabled && storageHandler.getLineageDir.isDefined,
      "No lineage directory defined. Use LineageContext.setLineageDir(dir) to save lineage" +
        " output."
    )

    val lineagePath = storageHandler.createNewLineageDirectory()

    val vertices = lineageContext.getMode() match {
      case samplingMode: SamplingMode => samplingMode.filter(g.vertices)
      case _ => g.vertices
    }

    val newG = Graph(vertices, g.edges)

    if (lineageContext.isTracingEnabled) {
      val path = f"${lineagePath}/${iteration}%04d.v"
      storageHandler.save(newG, path)
      iteration += 1
    }
  }
}
