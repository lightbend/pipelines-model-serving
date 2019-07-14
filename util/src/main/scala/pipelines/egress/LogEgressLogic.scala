package pipelines.egress

import pipelines.streamlets._
import pipelines.akkastream._
import pipelines.akkastream.scaladsl._
import akka.actor.ActorSystem
import akka.event.{ Logging => AkkaLogging }
import akka.event.Logging.{ LogLevel => AkkaLogLevel, InfoLevel => AkkaInfoLevel }
import pipelines.logging.{ AkkaLogger, LoggingUtil, MutableLogger, StdoutStderrLogger }
import pipelines.config.ConfigUtil
import pipelines.config.ConfigUtil.implicits._

/** Abstraction for writing to logs using Akka Logging. */
final case class LogEgressLogic[IN](
  in: CodecInlet[IN],
  logLevel: AkkaLogLevel,
  prefix: String)(implicit context: StreamletContext)
  extends FlowEgressLogic[IN](in) {

  // We actually use the LoggingUtil wrapper, so we can override the behavior in tests.
  // We start with the test-oriented "stdout/stderr" logger, then replace it inside
  // the flow...
  val logger: MutableLogger = new MutableLogger(StdoutStderrLogger)

  def flowWithContext(system: ActorSystem) = {
    logger.setLogger(AkkaLogger(system, logLevel))
    FlowWithPipelinesContext[IN].map { message ⇒
      logger.log(s"$prefix: $message")
      throw new RuntimeException(s"$prefix: $message")
      message
    }
  }
}

object LogEgressLogic {

  /** Make an object using a specific log level. */
  def make[IN](
    in: CodecInlet[IN],
    logLevel: AkkaLogLevel = AkkaInfoLevel,
    prefix: String = "")(implicit context: StreamletContext) =
    new LogEgressLogic[IN](in, logLevel, prefix)

  /** Make an object using a log level loaded from the configuration. */
  def makeFromConfig[IN](
    in: CodecInlet[IN],
    logLevelConfigKey: String,
    prefix: String = "")(implicit context: StreamletContext) = {
    val levelString = ConfigUtil.default.getOrElse[String](logLevelConfigKey)("info")
    val level = AkkaLogging.levelFor(levelString) match {
      case Some(level) => level
      case None =>
        // TODO: It would be better to log this message ;)
        Console.err.println("WARNING: Invalid LogLevel name found for configuration key $logLevelConfigKey. Using InfoLevel.")
        AkkaInfoLevel
    }
    LogEgressLogic[IN](in, level, prefix)
  }
}
