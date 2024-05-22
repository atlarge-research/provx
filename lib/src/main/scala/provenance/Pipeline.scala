package lu.magalhaes.gilles.provxlib
package provenance

import provenance.events.{EventType, Operation}
import provenance.metrics.{ObservationSet, TimeUnit}

import lu.magalhaes.gilles.provxlib.provenance.query.{
  DeltaPredicate,
  GraphPredicate
}
import org.apache.spark.graphx.Graph

import scala.reflect.ClassTag

object Pipeline {

  def trace[VD: ClassTag, ED: ClassTag, VD1: ClassTag, ED1: ClassTag](
      source: GraphLineage[VD, ED],
      event: EventType
  )(f: => GraphLineage[VD1, ED1]): GraphLineage[VD1, ED1] =
    ProvenanceContext.tracingEnabled match {
      case false => f
      case true =>
        ProvenanceContext.hooks.handlePre(event, source)
        val startTime = System.nanoTime()
        val res = f
        val endTime = System.nanoTime()
        val o = ObservationSet()
        o.add(TimeUnit("operationTime", endTime - startTime, "ns"))
        ProvenanceContext.hooks.handlePost(event, res)
        ProvenanceContext.graph.add(
          source,
          res,
          ProvenanceGraph.Edge(event, o)
        )
        val lineageGraph = if (ProvenanceContext.captureFilter.isDefined) {
          ProvenanceContext.graph.filter(
            nodeP =
              ProvenanceContext.captureFilter.get.provenanceFilter.nodePredicate,
            edgeP =
              ProvenanceContext.captureFilter.get.provenanceFilter.edgePredicate
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
            e.outer.output.g == res && e.outer.edge.event == event && ProvenanceContext.captureFilter.get.provenanceFilter
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
          case _ => true
        }

        if (!storeGraph) {
          return res
        }

        // Save graph when capture query results
        if (!queryResult) {
          return res
        }

        // matched the graph from provenance perspective, but still need to filter it here and then save

        val filteredDataGraph =
          ProvenanceContext.captureFilter.get.dataFilter match {
            case DeltaPredicate(ids) =>
              val newNodes = source.vertices.join(ids).mapValues(v => v._1)
//              val newNodes =
//                source.vertices.sample(withReplacement = false, 0.1)

              Graph(newNodes, source.edges)

            case GraphPredicate(nodePredicate, edgePredicate) =>
              res.graph.subgraph(
                epred = edgePredicate,
                vpred = nodePredicate
              )
            case _ => ???
          }

        // TODO: should we add this to the provenance graph?
        val filteredDataGraphGL = GraphLineage(filteredDataGraph)

        if (ProvenanceContext.sparkContext.isDefined) {
          println("SparkSession defined in ProvenanceContext")
          val storageLoc = ProvenanceContext.storageHandler.save(
            ProvenanceContext.sparkContext.get,
            filteredDataGraphGL
          )
          res.setStorageLocation(storageLoc)
        }

        res
    }
}
