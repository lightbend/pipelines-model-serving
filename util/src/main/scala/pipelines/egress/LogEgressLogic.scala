package pipelines.egress

import pipelines.streamlets._
import pipelines.akkastream._
import pipelines.akkastream.scaladsl._
import akka.actor.ActorSystem
import akka.event.Logging
import akka.event.Logging.{ LogLevel, InfoLevel }
import pipelines.util.ConfigUtil
import pipelines.util.ConfigUtil.implicits._

/** Abstraction for writing to logs. */
final case class LogEgressLogic[IN](
  in: CodecInlet[IN],
  logLevel: LogLevel,
  prefix: String)(implicit context: StreamletContext)
  extends FlowEgressLogic[IN](in) {

  def flowWithContext(system: ActorSystem) =
    FlowWithPipelinesContext[IN].map { message ⇒
      system.log.log(logLevel, s"$prefix: $message")
      message
    }
}

object LogEgressLogic {

  /** Make an object using a specific log level. */
  def make[IN](
    in: CodecInlet[IN],
    logLevel: LogLevel = InfoLevel,
    prefix: String = "")(implicit context: StreamletContext) =
    new LogEgressLogic[IN](in, logLevel, prefix)

  /** Make an object using a log level loaded from the configuration. */
  def makeFromConfig[IN](
    in: CodecInlet[IN],
    logLevelConfigKey: String,
    prefix: String = "")(implicit context: StreamletContext) = {
    val levelString = ConfigUtil.default.getOrElse[String](logLevelConfigKey)("info")
    val level = Logging.levelFor(levelString) match {
      case Some(level) => level
      case None =>
        // TODO: It would be better to log this message ;)
        Console.err.println("WARNING: Invalid LogLevel name found for configuration key $logLevelConfigKey. Using InfoLevel.")
        InfoLevel
    }
    new LogEgressLogic[IN](in, level, prefix)
  }
}
