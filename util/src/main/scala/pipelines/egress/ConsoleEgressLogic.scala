package pipelines.egress

import pipelines.streamlets._
import pipelines.akkastream._
import pipelines.akkastream.scaladsl._

/**
 * Abstraction for writing to output to the console (i.e., stdout).
 * @param in CodecInlet for records of type IN
 * @param prefix prepended to each record. _Add punctuation and whitespace if you want it_
 */
final case class ConsoleEgressLogic[IN](
  in: CodecInlet[IN], prefix: String)(
  implicit
  context: StreamletContext) extends FlowEgressLogic[IN](in) {

  def write(record: IN): Unit = print(prefix + record.toString)
}

object ConsoleEgressLogic {

  def make[IN](in: CodecInlet[IN], prefix: String = "")(
    implicit
    context: StreamletContext) =
    new ConsoleEgressLogic[IN](in, prefix)
}
