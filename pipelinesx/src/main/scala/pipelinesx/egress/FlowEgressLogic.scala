package pipelinesx.egress

import pipelines.streamlets._
import pipelines.akkastream._
import pipelines.akkastream.scaladsl._
import akka.stream.scaladsl.Sink

/**
 * An abstraction for the logic in an "Egress" that has a single inlet and then
 * "disposes" of the data in some way that's transparent to Pipelines, e.g.,
 * logs it or writes it to the console.
 * Note that Akka Streams at-most once semantics are used, so don't use this where
 * at-least once is needed, e.g., writing data to a database.
 */
abstract class FlowEgressLogic[IN](
    val inlet: CodecInlet[IN])(
    implicit
    context: StreamletContext)
  extends RunnableGraphStreamletLogic {
  /**
   * Logic to process the data, such as writing to a database.
   * Note that Akka Streams at-least once semantics are used, so implementations
   * may need to perform deduplication.
   */
  def write(record: IN): Unit

  def runnableGraph =
    atMostOnceSource(inlet).to(Sink.foreach(write))
}
