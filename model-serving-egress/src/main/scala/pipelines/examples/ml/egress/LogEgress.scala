package pipelines.examples.ml.egress

import pipelines.streamlets.avro._
import pipelines.akkastream.scaladsl._
import akka.actor.ActorSystem
import akka.event.Logging.LogLevel

import scala.reflect.ClassTag
import org.apache.avro.specific.SpecificRecordBase

abstract class LogEgress[IN <: SpecificRecordBase: ClassTag](logLevel: LogLevel)
  extends FlowEgress[IN](AvroInlet[IN]("in")) {

  /** String to write at the beginning of each line/ */
  def prefix: String

  def flowWithContext(system: ActorSystem) =
    FlowWithPipelinesContext[IN].map { message ⇒
      system.log.log(logLevel, s"$prefix: $message")
      message
    }
}
