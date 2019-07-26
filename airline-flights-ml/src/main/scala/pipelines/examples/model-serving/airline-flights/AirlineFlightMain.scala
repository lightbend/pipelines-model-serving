package pipelines.examples.modelserving.airlineflights

import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import pipelinesx.ingress.RecordsReader
import com.lightbend.modelserving.model.ServingResult
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import akka.actor.ActorSystem

/**
 * For testing the logic outside of Pipelines. Try -h or --help for information
 * Use sbt console, not sbt runMain:
 * ```
 * import pipelines.examples.modelserving.airlineflights._
 * AirlineFlightMain.main(Array("-n", "20"))
 * ...
 * ```
 */
object AirlineFlightMain {
  val defaultN = 100
  implicit val askTimeout: Timeout = Timeout(30.seconds)

  case class Options(count: Int)

  def main(args: Array[String]): Unit = {
      def parseArgs(args2: Seq[String], options: Options): Options = args2 match {
        case Nil ⇒ options
        case ("-h" | "--help") +: _ ⇒
          help()
          sys.exit(0)
        case ("-n" | "--count") +: x +: tail ⇒ toInt(x) match {
          case Some(n) ⇒ parseArgs(tail, options.copy(count = n))
          case _ ⇒
            println(s"ERROR: Invalid argument $x. (args = ${args.mkString(" ")}")
            help()
            sys.exit(1)
        }
        case x +: _ ⇒
          println(s"ERROR: Invalid argument $x. (args = ${args.mkString(" ")}")
          help()
          sys.exit(1)
      }

    val options = parseArgs(args, Options(defaultN))
    println("AirlineFlightMain: Running airlines test application.")
    println(s"Printing a maximum of ${options.count} flight records")

    implicit val system = ActorSystem("AirlineFlightMain")
    val modelServer = AirlineFlightModelServer.makeModelServer(system)
    val reader = RecordsReader.fromConfiguration[AirlineFlightRecord](
      configurationKeyRoot = AirlineFlightRecordIngressUtil.rootConfigKey,
      dropFirstN = 1)(
      AirlineFlightRecordIngressUtil.parse)
    (1 to options.count).foreach { n ⇒
      val (_, record) = reader.next()
      val resultFuture = modelServer.ask(record).mapTo[ServingResult[AirlineFlightResult]]
      val result = Await.result(resultFuture, 2 seconds)
      if (result.errors.length == 0)
        println(s"$n: scoring returned an error: ${result.errors} (full result: ${result})")
      else
        println(s"$n: scoring successful:: ${result.result} (full result: ${result})")

      Thread.sleep(100)
    }

    sys.exit(0)
  }

  def help(): Unit = {
    println(s"""
      |Tests airline flights data app.
      |usage: scala ...AirlineFlightMain [-h|--help] [-n|--count N]
      |where:
      |  -h | --help       print this message and quits
      |  -n | --count N    print this number of flight records (default: $defaultN)
      |""".stripMargin)
  }

  def toInt(n: String): Option[Int] =
    try {
      Some(n.toInt)
    } catch {
      case _: java.lang.NumberFormatException ⇒ None
    }
}
