package pipelines.egress

import pipelines.streamlets._
import pipelines.akkastream.scaladsl._
import akka.actor.ActorSystem
import akka.event.Logging.LogLevel

trait LogEgress extends FlowEgress {
  val logLevel: LogLevel
  val prefix: String

  def flowWithContext(system: ActorSystem) =
    FlowWithPipelinesContext[IN].map { message ⇒
      system.log.log(logLevel, s"$prefix: $message")
      message
    }
}
