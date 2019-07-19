package pipelines.examples.modelserving.airlineflights

import pipelines.examples.modelserving.airlineflights.data._
import pipelinesx.ingress.RecordsReader

/**
 * For testing the logic outside of Pipelines. Try -h or --help for information
 */
object AirlineFlightsMain {
  val defaultN = 100

  case class Options(count: Int)

  def main(args: Array[String]): Unit = {
    def parseArgs(args2: Seq[String], options: Options): Options = args2 match {
      case Nil => options
      case ("-h" | "--help") +: tail =>
        help()
        sys.exit(0)
      case ("-n" | "--count") +: x +: tail => toInt(x) match {
        case Some(n) => parseArgs(tail, options.copy(count = n))
        case _ =>
          println(s"ERROR: Invalid argument $x. (args = ${args.mkString(" ")}")
          help()
          sys.exit(1)
      }
      case x +: tail =>
        println(s"ERROR: Invalid argument $x. (args = ${args.mkString(" ")}")
        help()
        sys.exit(1)
    }

    val options = parseArgs(args, Options(defaultN))
    println("AirlineFlightsMain: Running airlines test application.")
    println(s"Printing a maximum of ${options.count} flight records")

    val server = new AirlineFlightModelServer()
    val reader = RecordsReader.fromConfiguration[AirlineFlightRecord](
      configurationKeyRoot = AirlineFlightRecordsIngressUtil.rootConfigKey,
      dropFirstN = 1)(
      AirlineFlightRecordsIngressUtil.parse)
    (1 to options.count).foreach { n ⇒
      val (_, record) = reader.next()
      val result = server.score(record)
      println("%7d: %s".format(n, result))
      Thread.sleep(100)
    }
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
      case _: java.lang.NumberFormatException => None
    }
}
