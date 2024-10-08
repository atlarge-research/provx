package lu.magalhaes.gilles.provxlib
package provenance

import provenance.events.{
  PregelAlgorithm,
  PregelIteration,
  PregelLifecycleStart
}
import provenance.hooks.PregelHook
import provenance.metrics.{Gauge, ObservationSet}

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object LineagePregel extends Logging {
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag](
      gl: GraphLineage[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either
  )(
      vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A
  ): GraphLineage[VD, ED] =
    Pipeline.trace(gl, PregelAlgorithm()) {
      require(
        maxIterations > 0,
        s"Maximum number of iterations must be greater than 0, but got ${maxIterations}"
      )

      var g = gl.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg))
      ProvenanceContext.graph.add(
        gl,
        g,
        ProvenanceGraph.Edge(PregelLifecycleStart(), ObservationSet())
      )

      // compute the messages
      var messages = mapReduceTriplets(g, sendMsg, mergeMsg)
      var activeMessages = messages.count()

      // Get all PregelEventHooks
      val hooks = ProvenanceContext.hooks.all
        .filter {
          case _: PregelHook => true
          case _             => false
        }
        .map(_.asInstanceOf[PregelHook])

      // Run pre-start hooks
      hooks.foreach(_.preStart(g))

      // Loop
      var prevG: GraphLineage[VD, ED] = null
      var i = 0
      while (activeMessages > 0 && i < maxIterations) {

        hooks.foreach(_.preIteration(g))

        prevG = g
        g = Pipeline.trace(g, PregelIteration(i)) {
          g.joinVertices(messages)(vprog)
        }

        // Receive the messages and update the vertices.
        val oldMessages = messages
        // Send new messages, skipping edges where neither side received a message. We must cache
        // messages so it can be materialized on the next line, allowing us to uncache the previous
        // iteration.
        messages = mapReduceTriplets(g, sendMsg, mergeMsg)
        // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
        // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
        // and the vertices of g).
        activeMessages = messages.count()
        g.metrics.add(Gauge("activeMessages", activeMessages))

        logInfo("Pregel finished iteration " + i)

        // Unpersist the RDDs hidden by newly-materialized RDDs
        oldMessages.unpersist()
        prevG.unpersistVertices()
        prevG.edges.unpersist()

        // Run post-iteration hooks
        hooks.foreach(_.postIteration(g))

        i += 1
      }

      // Run post-stop hooks
      hooks.foreach(_.postStop(g))

      // TODO: only unpersist when lineage data is not needed
      g
    } // end of apply

  // Copied from GraphX source, since needed to access to private mapReduceTriplets method
  private def mapReduceTriplets[VD: ClassTag, ED: ClassTag, A: ClassTag](
      gl: GraphLineage[VD, ED],
      mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      reduceFunc: (A, A) => A
  ): VertexRDD[A] = {
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

    gl.aggregateMessages(sendMsg, reduceFunc)
  }
}
