package pipelinesx.ingress

import akka._
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.stream._
import akka.stream.scaladsl._

/**
 * A StreamletLogic that splits custom source into two of outputs
 */
class InputTrafficSplitter[T](
    inlet:   SourceWithContext[(Int, T), CommittableOffset, NotUsed],
    outlets: Seq[Sink[(T, CommittableOffset), NotUsed]]
) {

  def runnableGraph() = {

    RunnableGraph.fromGraph(
      GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] ⇒
        import GraphDSL.Implicits._

        // The partitioning lambda
        val partition = builder.add(Partition[((Int, T), CommittableOffset)](outlets.size, message ⇒ {
          message._1._1
        }))

        inlet ~> partition.in

        0 until outlets.size foreach (i ⇒ partition.out(i).map(m => (m._1._2, m._2)) ~> outlets(i))

        ClosedShape
      }
    )
  }
}
