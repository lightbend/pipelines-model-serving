package pipelinesx.ingress

import akka._
import akka.stream._
import akka.stream.contrib.PartitionWith
import akka.stream.scaladsl._
import akka.kafka.ConsumerMessage.CommittableOffset

/**
 * A StreamletLogic that splits custom source into two of outputs
 */
abstract class InputTrafficSplitter[T](
    inlet:   SourceWithContext[Either[T, T], CommittableOffset, NotUsed],
    outlet1: Sink[(T, CommittableOffset), NotUsed],
    outlet2: Sink[(T, CommittableOffset), NotUsed]
) {

  def runnableGraph() = {

    RunnableGraph.fromGraph(
      GraphDSL.create(outlet1, outlet2)(Keep.left) { implicit builder: GraphDSL.Builder[NotUsed] ⇒ (o1, o2) ⇒
        import GraphDSL.Implicits._

        val partitionWith = PartitionWith[(Either[T, T], CommittableOffset), (T, CommittableOffset), (T, CommittableOffset)] {
          case (Left(e), offset)  ⇒ Left((e, offset))
          case (Right(e), offset) ⇒ Right((e, offset))
        }
        val partitioner = builder.add(partitionWith)

          // format: OFF
          inlet ~>  partitioner.in
          partitioner.out0 ~> o1
          partitioner.out1 ~> o2
        // format: ON

        ClosedShape
      }
    )
  }
}
