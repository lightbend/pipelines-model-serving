package pipelines.egress

import pipelines.streamlets._
import pipelines.akkastream._
import pipelines.akkastream.scaladsl._
import scala.reflect.ClassTag
import org.apache.avro.specific.SpecificRecordBase
import akka.actor.ActorSystem

/**
 * An abstraction for an "Egress" that has a single inlet and then "disposes" of
 * the data in some way that's transparent to Pipelines, e.g., log it, write it to
 * to a database, or write it to the console.
 * Note that Akka Streams at-least once semantics are used, so subclasses that
 * implement the `flowWithContext` method may wish to implement deduplication.
 */
abstract class FlowEgress[IN <: SpecificRecordBase: ClassTag](
  val in: CodecInlet[IN]) extends AkkaStreamlet {

  final override val shape = StreamletShape.withInlets(in)

  /**
   * Logic to process the data, such as writing to a database.
   * Note that Akka Streams at-least once semantics are used, so implementations
   * may need to perform deduplication.
   */
  def flowWithContext(system: ActorSystem): FlowWithPipelinesContext[IN, IN]

  override def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      atLeastOnceSource(in)
        .via(flowWithContext(system).asFlow)
        .to(atLeastOnceSink)
  }
}
