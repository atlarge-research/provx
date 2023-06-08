package lu.magalhaes.gilles.provxlib
package lineage

import lineage.metrics.{Counter, Gauge, ObservationSet}

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object LineagePregel extends Logging {
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either,
   pruneLineage: Option[((VertexId, VD)) => Boolean] = None,
   sampleFraction: Option[Double] = None)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : (Graph[VD, ED], ObservationSet) = {
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg))

    val checkpointer = new GraphCheckpointer[VD, ED](
      g.vertices.sparkContext,
      pruneLineage, sampleFraction
    )
    //    val graphCheckpointer = new PeriodicGraphCheckpointer[VD, ED](
//      checkpointInterval, graph.vertices.sparkContext)
//    graphCheckpointer.update(g)

    // compute the messages
    var messages = mapReduceTriplets(g, sendMsg, mergeMsg)
//    val messageCheckpointer = new PeriodicRDDCheckpointer[(VertexId, A)](
//      checkpointInterval, graph.vertices.sparkContext)
//    messageCheckpointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])
    var activeMessages = messages.count()

    checkpointer.save(g)

    val metrics = new ObservationSet()

    // Loop
    var prevG: Graph[VD, ED] = null
    var i = Counter.zero
//    var i = 0
    // TODO: add iteration time
    while (activeMessages > 0 && i.current < maxIterations) {
      // Receive the messages and update the vertices.
      prevG = g
      g = g.joinVertices(messages)(vprog)
      checkpointer.save(g)

      val generation = new ObservationSet()

      println(s"Iteration ${i.current}: Sending ${messages.count()} messages.")

      val oldMessages = messages
      // Send new messages, skipping edges where neither side received a message. We must cache
      // messages so it can be materialized on the next line, allowing us to uncache the previous
      // iteration.
      messages = mapReduceTriplets(
        g, sendMsg, mergeMsg, Some((oldMessages, activeDirection)))
      // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
      // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
      // and the vertices of g).
//      messageCheckpointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])
      activeMessages = messages.count()
      generation.add(new Gauge("activeMessages", activeMessages))

      logInfo("Pregel finished iteration " + i)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist()
      prevG.unpersistVertices()
      prevG.edges.unpersist()
      // count the iteration
      i = i.increment()
      generation.add(i)
      metrics.add(generation)
    }
//    messageCheckpointer.unpersistDataSet()
//    graphCheckpointer.deleteAllCheckpoints()
//    messageCheckpointer.deleteAllCheckpoints()
    (g, metrics)
  } // end of apply

  private def mapReduceTriplets[VD: ClassTag, ED: ClassTag, A: ClassTag](
      g: Graph[VD, ED],
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      reduceFunc: (A, A) => A,
      activeSetOpt: Option[(VertexRDD[_], EdgeDirection)] = None): VertexRDD[A] = {
    def sendMsg(ctx: EdgeContext[VD, ED, A]): Unit = {
      mapFunc(ctx.toEdgeTriplet).foreach { kv =>
        val id = kv._1
        val msg = kv._2
        if (id == ctx.srcId) {
          ctx.sendToSrc(msg)
        } else {
          assert(id == ctx.dstId)
          ctx.sendToDst(msg)
        }
      }
    }

    g.aggregateMessages(sendMsg, reduceFunc)
  }
}
