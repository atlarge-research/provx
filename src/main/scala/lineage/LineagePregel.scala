package lu.magalhaes.gilles.provxlib
package lineage

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.EdgeActiveness
import org.apache.spark.internal.Logging

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
  : (Graph[VD, ED], PregelMetrics) = {
    // TODO: make setting lineage directory optional
    require(
      LineageContext.getLineageDir.isDefined,
      "No lineage directory defined. Use LineageContext.setLineageDir(dir) to save lineage " +
        " output."
    )
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")

    val lineagePath = LineageContext.getLineageDir.get

//    val checkpointInterval = graph.vertices.sparkContext.getConf
//      .getInt("spark.graphx.pregel.checkpointInterval", -1)
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg))
//    val graphCheckpointer = new PeriodicGraphCheckpointer[VD, ED](
//      checkpointInterval, graph.vertices.sparkContext)
//    graphCheckpointer.update(g)

    // compute the messages
    var messages = mapReduceTriplets(g, sendMsg, mergeMsg)
//    val messageCheckpointer = new PeriodicRDDCheckpointer[(VertexId, A)](
//      checkpointInterval, graph.vertices.sparkContext)
//    messageCheckpointer.update(messages.asInstanceOf[RDD[(VertexId, A)]])
    var activeMessages = messages.count()

    if (LineageContext.isCheckpointingEnabled()) {
      g.vertices.saveAsTextFile(f"${lineagePath}/input.v")
    }

    val metrics = new PregelMetrics()

    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages and update the vertices.
      prevG = g
      g = g.joinVertices(messages)(vprog)
//      graphCheckpointer.update(g)

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

      if (LineageContext.isCheckpointingEnabled()) {
        vertices.saveAsTextFile(f"${lineagePath}/${i}%04d.v")
      }

      // scalastyle:off println
      println(s"Iteration ${i}: Sending ${messages.count()} messages.")
      // scalastyle:on println

      metrics.updateMetrics(new PregelIterationMetrics(messages.count()))

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

      logInfo("Pregel finished iteration " + i)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist()
      prevG.unpersistVertices()
      prevG.edges.unpersist()
      // count the iteration
      i += 1
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
