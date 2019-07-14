package pipelines.examples.airlineflights.main

import pipelines.examples.modelserving.AirlineFlightModelServer
import pipelines.examples.ingestor.AirlineFlightRecordsIngressUtil
import pipelines.examples.data._
import pipelines.ingress.RecordsReader

/**
 * For testing the logic outside of Pipelines.
 * WARNING: For some reason, the application.conf file is NOT on the CLASSPATH when
 * you use `sbt runMain ...` Instead, use this to test it (where N=100 is used):
 * ```
 * > sbt console
 * scala> import pipelines.examples.airlineflights.main.Main
 * scala> Main.main(Array("100"))
 * ```
 */
object Main {
  val defaultN = 1000

  def main(args: Array[String]): Unit = {
    def parseArgs(args2: Seq[String], count: Int): Int = args2 match {
      case Nil => count
      case ("-h" | "--help") +: tail =>
        help()
        sys.exit(0)
      case x +: tail => toInt(x) match {
        case Some(n) => parseArgs(tail, n)
        case _ =>
          println(s"ERROR: Invalid argument $x. (args = ${args.mkString(" ")}")
          help()
          sys.exit(1)
      }
    }

    val count = parseArgs(args, defaultN)
    println(s"Main: Running airlines test application. Printing a maximum of $count lines")
    val server = new AirlineFlightModelServer()
    val reader = RecordsReader.fromClasspath[AirlineFlightRecord](
      resourcePaths = AirlineFlightRecordsIngressUtil.airlineFlightRecordsResources,
      dropFirstN = 1)(
      AirlineFlightRecordsIngressUtil.parse)
    (1 to count).foreach { n ⇒
      val (_, record) = reader.next()
      val result = server.score(record)
      println("%7d: %s".format(n, result))
      Thread.sleep(100)
    }
  }

  def help(): Unit = {
    println(s"""
      |usage: scala ...Main [-h|--help] [N]
      |where:
      |  -h | --help   prints this message and quits
      |  N             the number of records to parse and print (default: $defaultN)
      |""".stripMargin)
  }

  def toInt(n: String): Option[Int] =
    try {
      Some(n.toInt)
    } catch {
      case _: java.lang.NumberFormatException => None
    }
}
