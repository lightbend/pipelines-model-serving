package pipelines.logging

import org.slf4j.{ Logger => SLogger, LoggerFactory => SLoggerFactory }
import akka.actor.ActorSystem
import akka.event.{ Logging => ALogging }
import akka.event.Logging.{ LogLevel => ALogLevel, InfoLevel => AInfoLevel }

/**
 * Wrapper around logging to support SLF4J, Akka Logging, and stdout. The _only_
 * reason this exists is to make it easier to capture logged output during tests
 * for verification or to suppress the output (after all other attempts at this
 * failed...).
 */
trait Logger {
  /** See implementations for details on behavior. */
  def log(msg: String): Unit
  def debug(msg: String): Unit
  def info(msg: String): Unit
  def warn(msg: String): Unit
  def error(msg: String): Unit
}

final class MutableLogger(var logger: Logger) extends Logger {
  def setLogger(l: Logger): Unit = logger = l

  def log(msg: String): Unit = logger.log(msg)
  def debug(msg: String): Unit = logger.debug(msg)
  def info(msg: String): Unit = logger.info(msg)
  def warn(msg: String): Unit = logger.warn(msg)
  def error(msg: String): Unit = logger.error(msg)
}

final case class SLF4JLogger(logger: SLogger) extends Logger {
  /** Since there is no generic "log" method in the SLF4J logger, this method logs as INFO. */
  def log(msg: String): Unit = logger.info(msg)
  def debug(msg: String): Unit = logger.debug(msg)
  def info(msg: String): Unit = logger.info(msg)
  def warn(msg: String): Unit = logger.warn(msg)
  def error(msg: String): Unit = logger.error(msg)
}

final case class AkkaLogger(system: ActorSystem, level: ALogLevel) extends Logger {
  /** Uses the LogLevel passed in as a constructor argument. */
  def log(msg: String): Unit = system.log.log(level, msg)
  def debug(msg: String): Unit = system.log.debug(msg)
  def info(msg: String): Unit = system.log.info(msg)
  def warn(msg: String): Unit = system.log.warning(msg)
  def error(msg: String): Unit = system.log.error(msg)
}

/** Writes debug and info messages to stdout, warn and error messages to stderr */
final case object StdoutStderrLogger extends Logger {
  /** Treats the message as an INFO message. */
  def log(msg: String): Unit = Console.out.println(s"INFO:  $msg")
  def debug(msg: String): Unit = Console.out.println(s"DEBUG: $msg")
  def info(msg: String): Unit = Console.out.println(s"INFO:  $msg")
  def warn(msg: String): Unit = Console.err.println(s"WARN:  $msg")
  def error(msg: String): Unit = Console.err.println(s"ERROR: $msg")
}

object LoggingUtil {

  /**
   * Always call this method to create the default SL4J logger, wrapped in a [[MutableLogger]].
   * To override in a test to use stdout, call `mylogger.setLogger(StdoutStderrLogger)`.
   */
  def getLogger[T](clazz: Class[T]): MutableLogger =
    new MutableLogger(SLF4JLogger(SLoggerFactory.getLogger(clazz)))
}
