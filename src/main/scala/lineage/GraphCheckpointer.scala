package lu.magalhaes.gilles.provxlib
package lineage

import lineage.modes.SamplingExecutionMode

import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

class GraphCheckpointer[VD: ClassTag, ED: ClassTag]() {

  private var iteration = 0

//  private val uniqueDirectory = UUID.randomUUID().toString

  def save(gl: GraphLineage[VD, ED]): Unit = {
//    if (!lineageContext.isTracingEnabled) {
//      return
//    }
//
//    val storageHandler = lineageContext.getStorageHandler
//
//    require(
//      lineageContext.isTracingEnabled && storageHandler.getLineageDir.isDefined,
//      "No lineage directory defined. Use LineageContext.setLineageDir(dir) to save lineage" +
//        " output."
//    )
//
//    val vertices = LineageContext.getMode() match {
//      case samplingMode: SamplingExecutionMode => samplingMode.filter(gl)
//      case _ => gl.vertices
//    }
//
//    val newG = Graph(vertices, gl.edges)
//
//    if (LineageContext.isTracingEnabled) {
//      storageHandler.save(newG)
//    }
  }
}
