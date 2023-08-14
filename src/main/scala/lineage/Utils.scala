package lu.magalhaes.gilles.provxlib
package lineage

import scala.reflect.ClassTag

object Utils {

  def trace[VD: ClassTag, ED: ClassTag, VD1: ClassTag, ED1: ClassTag](
      source: GraphLineage[VD, ED], event: EventType)(f: => GraphLineage[VD1, ED1]): GraphLineage[VD1, ED1] = {
    if (source == null) {
      return f
    }
    val edge = ProvenanceGraph.Relation(ProvenanceGraph.Node(Some(source)), ProvenanceGraph.Node(None), event)
    LineageContext.hooks.handlePre(edge, source)
    val res = f
    LineageContext.hooks.handlePost(edge, source)
    LineageContext.graph.add(source, res, event)
    res
  }

}
