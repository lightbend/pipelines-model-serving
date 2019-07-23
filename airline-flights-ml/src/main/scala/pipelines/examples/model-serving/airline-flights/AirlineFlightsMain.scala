package pipelines.examples.modelserving.airlineflights

import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import pipelinesx.ingress.RecordsReader
import com.lightbend.modelserving.model.ServingResult
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout

/**
 * For testing the logic outside of Pipelines. Try -h or --help for information
 */
object AirlineFlightsMain {
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
    println("AirlineFlightsMain: Running airlines test application.")
    println(s"Printing a maximum of ${options.count} flight records")

    val modelServer = AirlineFlightModelServer.makeModelServer()
    val reader = RecordsReader.fromConfiguration[AirlineFlightRecord](
      configurationKeyRoot = AirlineFlightRecordsIngressUtil.rootConfigKey,
      dropFirstN = 1)(
      AirlineFlightRecordsIngressUtil.parse)
    (1 to options.count).foreach { n ⇒
      val (_, record) = reader.next()
      val resultFuture = modelServer.ask(record).mapTo[ServingResult[AirlineFlightResult]]
      val result = Await.result(resultFuture, 2 seconds)
      result.result match {
        case None    ⇒ println(s"$n: Received a None in the result: $result")
        case Some(r) ⇒ println(s"$n: Received result: $r")
      }
      Thread.sleep(100)
    }

    sys.exit(0)
  }

  def help(): Unit = {
    println(s"""
      |Tests airline flights data app.
      |usage: scala ...AirlineFlightsMain [-h|--help] [-n|--count N]
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
