package lu.magalhaes.gilles.provxlib
package lineage

import org.apache.spark.graphx.{Graph, VertexId}

import scala.reflect.ClassTag

class GraphCheckpointer[VD: ClassTag, ED: ClassTag](
   pruneLineage: Option[((VertexId, VD)) => Boolean] = None,
   sampleFraction: Option[Double] = None) {

  private var iteration = 0

  def save(g: Graph[VD, ED]): Unit = {
    require(
      LineageContext.isCheckpointingEnabled && LineageContext.getLineageDir.isEmpty,
      "No lineage directory defined. Use LineageContext.setLineageDir(dir) to save lineage " +
        " output."
    )

    val lineagePath = LineageContext.getLineageDir.get

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
      vertices.saveAsTextFile(f"${lineagePath}/${iteration}%04d.v")
      iteration += 1
    }
  }
}
