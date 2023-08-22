package lu.magalhaes.gilles.provxlib
package provenance

import provenance.events.{EventType, Operation}

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

      if (res.storageLocation.isDefined) {
        println(s"Storage location already defined for G${res.id}")
        return res
      }

      val queryResult =
        lineageGraph.graph.edges.count((e: ProvenanceGraph.Type#EdgeT) => {
          e.outer.output.g == res && e.outer.event == event && res.captureFilter.get.provenanceFilter
            .edgePredicate(e.outer)
        }) == 1
//      println(
//        s"query result defined: ${queryResult} ${res.id} ${event.toString}"
//      )

      val storeGraph = event match {
        case Operation(name) =>
          name match {
            // We cannot store something we're trying to remove
            case "unpersist" | "unpersistVertices" => false
            case _                                 => true
          }
      }

      if (!storeGraph) {
        return res
      }

      // Save graph when capture query results
      if (queryResult) {
        if (ProvenanceContext.sparkContext.isDefined) {
          println("SparkSession defined in ProvenanceContext")
          val storageLoc = ProvenanceContext.storageHandler.save(
            ProvenanceContext.sparkContext.get,
            res
          )
          res.setStorageLocation(storageLoc)
        }
//        val storageLoc = ProvenanceContext.storageHandler.save(res)
//        res.setStorageLocation(storageLoc)
      }
      res
    } else {
      f
    }
}
