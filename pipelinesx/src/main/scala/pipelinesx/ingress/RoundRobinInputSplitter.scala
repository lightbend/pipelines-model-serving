package pipelinesx.ingress

import akka._
import akka.stream._
import akka.stream.scaladsl._

/**
 * A StreamletLogic that splits custom source into a list of outputs
 */
abstract class RoundRobinInputSplitter[T]
  (outlets: Sink[T, _]*) {

  private var counter = -1

  /**
   * Defines the flow that produces elements of type [T]
   */
  def source: Source[T, NotUsed]

  def runnableGraph() = {

    RunnableGraph.fromGraph(
      GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] ⇒
        import GraphDSL.Implicits._

        // The partitioning lambda
        val partition = builder.add(Partition[T](outlets.size, _ ⇒ {
          counter = counter + 1
          if (counter >= outlets.size) counter = 0
          counter
        }))

        source ~> partition.in
        0 until outlets.size foreach (i ⇒ partition.out(i) ~> outlets(i))

        ClosedShape
      }
    )
  }
}
