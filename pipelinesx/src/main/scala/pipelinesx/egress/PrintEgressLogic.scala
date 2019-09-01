package pipelinesx.egress

import pipelines.streamlets._
import pipelines.akkastream._
import pipelines.akkastream.scaladsl._
import akka.stream.scaladsl.Sink

/**
 * An abstraction for the logic in an "Egress" that has a single inlet and then
 * "prints" of the data in some way that's transparent to Pipelines, e.g.,
 * logs it or writes it to the console.
 * Note that Akka Streams at-most once semantics are used, so don't use this where
 * at-least once is needed, e.g., writing data to a database.
 */
abstract class PrintEgressLogic[IN](
    val inlet: CodecInlet[IN])(
    implicit
    context: StreamletContext)
  extends RunnableGraphStreamletLogic {
  /**
   * Logic to "print" the data, such as writing to a log.
   * Note that Akka Streams at-most once semantics are used.
   */
  def write(record: IN): Unit

  def runnableGraph =
    atMostOnceSource(inlet).to(Sink.foreach(write))
}
