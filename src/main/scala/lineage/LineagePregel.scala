package lu.magalhaes.gilles.provxlib
package lineage

import lineage.metrics.{Gauge, ObservationSet}

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object LineagePregel extends Logging {
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (gl: GraphLineage[VD, ED],
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : GraphLineage[VD, ED] = {
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0, but got ${maxIterations}")

    val lineageContext = gl.lineageContext
    var g = gl.getGraph().mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg))

    val checkpointer = new GraphCheckpointer[VD, ED](lineageContext)
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

    val hooks = lineageContext.hooksRegistry.allHooks

    val metrics = ObservationSet()

    // Run pre-start hooks
    hooks.foreach(_.preStart(metrics))

    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      val generation = ObservationSet()

      hooks.foreach(_.preIteration(generation))

      // Receive the messages and update the vertices.
      prevG = g
      g = g.joinVertices(messages)(vprog)
      checkpointer.save(g)

      println(s"Iteration ${i}: Sending ${messages.count()} messages.")

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
      generation.add(Gauge("activeMessages", activeMessages))

      logInfo("Pregel finished iteration " + i)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist()
      prevG.unpersistVertices()
      prevG.edges.unpersist()

      // Run post-iteration hooks
      hooks.foreach(_.postIteration(generation))
      metrics.add(generation)

      i += 1
    }

    // Run post-stop hooks
    hooks.foreach(_.postStop(metrics))

    val newGl = new GraphLineage(g, gl.lineageContext)
    newGl.setMetrics(metrics)

    // TODO: only unpersist when lineage data is not needed
//    messageCheckpointer.unpersistDataSet()
//    graphCheckpointer.deleteAllCheckpoints()
//    messageCheckpointer.deleteAllCheckpoints()
    newGl
  } // end of apply

  // Copied from GraphX source, since needed to access to private mapReduceTriplets method
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
