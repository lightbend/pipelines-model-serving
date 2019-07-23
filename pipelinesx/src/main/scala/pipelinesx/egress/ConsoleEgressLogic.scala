package pipelinesx.egress

import pipelines.streamlets._
import pipelines.akkastream._

/**
 * Abstraction for writing to output to the console (i.e., stdout).
 * @param in CodecInlet for records of type IN
 * @param prefix prepended to each record. _Add punctuation and whitespace if you want it_
 */
final case class ConsoleEgressLogic[IN](
    in: CodecInlet[IN], prefix: String)(
    implicit
    context: StreamletContext) extends FlowEgressLogic[IN](in) {

  def write(record: IN): Unit = println(prefix + record.toString)
}
