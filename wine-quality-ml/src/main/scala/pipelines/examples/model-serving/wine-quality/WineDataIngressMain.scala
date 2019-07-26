package pipelines.examples.modelserving.winequality

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

/**
 * Test program for [[WineDataIngressUtil]]; reads wine records and prints them.
 */
object WineDataIngressMain {

  /**
   * For testing purposes.
   * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
   * the console instead:
   * ```
   * import pipelines.examples.modelserving.winequality._
   * WineDataIngressMain.main(Array("-n", "5"))
   * ```
   */
  def main(args: Array[String]): Unit = {
    val defaultN = 100
    val defaultF = WineDataIngressUtil.dataFrequencyMilliseconds
    def help() = println(s"""
      |usage: WineDataIngressMain [-h|--help] [-n|--count N] [-f|--frequency F]
      |where:
      |  -h | --help         print this message and exit
      |  -n | --count N      print N records and stop (default: $defaultN)
      |  -f | --frequency F  seconds between output model descriptions (default: $defaultF)
      |""".stripMargin)

    def parseArgs(args2: Seq[String], nf: (Int,Int)): (Int,Int) = args2 match {
      case ("-h" | "--help") +: _ ⇒
        help()
        sys.exit(0)
      case Nil                                 ⇒ nf
      case ("-n" | "--count") +: n +: tail ⇒ parseArgs(tail, (n.toInt, nf._2))
      case ("-f" | "--frequency") +: n +: tail ⇒ parseArgs(tail, (nf._1, n.toInt.seconds))
      case x +: _ ⇒
        println(s"ERROR: Unrecognized argument $x. All args: ${args.mkString(" ")}")
        help()
        sys.exit(1)
    }

    val (count, frequency) = parseArgs(args, (defaultN, defaultF))

    println(s"printing $count records")
    println(s"frequency (seconds): $frequency")
    implicit val system = ActorSystem("WineDataIngressMain")
    implicit val mat = ActorMaterializer()
    val source = WineDataIngressUtil.makeSource(
      WineDataIngressUtil.rootConfigKey, frequency)
    source.runWith {
      Sink.foreach { line ⇒
        println(line.toString)
        count -= 1
        if (count == 0) {
          println("Finished!")
          sys.exit(0)
        }
      }
    }
    println("Should never get here...")
  }
}

