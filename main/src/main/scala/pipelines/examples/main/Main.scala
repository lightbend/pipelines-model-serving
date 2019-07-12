package pipelines.examples.main

import pipelines.examples.modelserving.AirlineFlightModelServer
import pipelines.examples.ingestor.AirlineFlightRecordsIngressUtil
import pipelines.examples.data._
import pipelines.ingress.CSVReader

object Main {
  val defaultN = 1000

  def main(args: Array[String]): Unit = {
    def parseArgs(args2: Seq[String], opts: (Int, String)): (Int, String) = args2 match {
      case Nil => opts
      case ("-h" | "--help") +: tail =>
        help()
        sys.exit(0)
      case (whichOne @ ("air" | "airlines" | "flights")) +: tail => parseArgs(tail, (opts._1, "air"))
      case (whichOne @ ("wine" | "recommender")) +: tail => parseArgs(tail, (opts._1, whichOne))
      case x +: tail => toInt(x) match {
        case Some(n) => parseArgs(tail, (n, opts._2))
        case _ =>
          println(s"ERROR: Invalid argument $x. (args = ${args.mkString(" ")}")
          help()
          sys.exit(1)
      }
    }

    val (count, whichOne) = parseArgs(args, (defaultN, "air"))
    println(s"Main: Running application $whichOne. Printing a maximum of $count lines")
    val (reader, server) = whichOne match {
      case "air" =>
        val server = new AirlineFlightModelServer()
        val reader = CSVReader.fromClasspath[AirlineFlightRecord](
          resourcePaths = AirlineFlightRecordsIngressUtil.airlineFlightRecordsResources,
          separator = ",",
          dropFirstN = 1)(AirlineFlightRecordsIngressUtil.parse)
        (reader, server)
      case _ =>
        println(s"Support for $whichOne is not yet implemented. Sorry...")
        sys.exit(1)
    }
    (1 to count).foreach { n ⇒
      val (_, record) = reader.next()
      val result = server.score(record)
      println("%7d: %s".format(n, result))
      Thread.sleep(100)
    }
  }

  def help(): Unit = {
    println(s"""
      |usage: scala ...Main [-h|--help] [N] [air|airlines|flights | wine | recommender]
      |where:
      |  -h | --help                 prints this message and quits
      |  N                           the number of records to parse and print (default: $defaultN)
      |  air | airlines | flights    run the airline flights code (default)
      |  wine                        run the wine quality code
      |  recommender                 run the recommender code
      |""".stripMargin)
  }

  def toInt(n: String): Option[Int] =
    try {
      Some(n.toInt)
    } catch {
      case _: java.lang.NumberFormatException => None
    }
}
