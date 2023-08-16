package lu.magalhaes.gilles.provxlib
package provenance

import provenance.events.EventType

import scala.reflect.ClassTag

object Utils {

  def trace[VD: ClassTag, ED: ClassTag, VD1: ClassTag, ED1: ClassTag](
      source: GraphLineage[VD, ED],
      event: EventType
  )(f: => GraphLineage[VD1, ED1]): GraphLineage[VD1, ED1] =
    if (ProvenanceContext.isTracingEnabled) {
      ProvenanceContext.hooks.handlePre(event, source)
      val res = f
      ProvenanceContext.hooks.handlePost(event, res)
      ProvenanceContext.graph.add(source, res, event)
      val lineageGraph = if (res.captureFilter.isDefined) {
        ProvenanceContext.graph.filter(
          nodeP = res.captureFilter.get.provenanceFilter.nodePredicate,
          edgeP = res.captureFilter.get.provenanceFilter.edgePredicate
        )
      } else {
        println("WARN: not capturing anything")
        ProvenanceContext.graph.filter(
          nodeP = ProvenanceGraph.noNode,
          edgeP = ProvenanceGraph.noEdge
        )
      }

      val queryResult =
        lineageGraph.graph.edges.count((e: ProvenanceGraph.Type#EdgeT) => {
          e.outer.output.g == res && e.outer.event == event && res.captureFilter.get.provenanceFilter
            .edgePredicate(e.outer)
        }) == 1
      println(
        s"query result defined: ${queryResult} ${res.id} ${event.toString}"
      )

      // Save graph when capture query results
      if (queryResult) {
        val storageLoc = ProvenanceContext.storageHandler.save(res)
        res.setStorageLocation(storageLoc)
      }
      res
    } else {
      f
    }
}
