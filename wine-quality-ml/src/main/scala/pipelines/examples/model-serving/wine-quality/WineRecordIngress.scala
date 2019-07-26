package pipelines.examples.modelserving.winequality

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelines.examples.modelserving.winequality.data.WineRecord
import pipelinesx.ingress.RecordsReader
import pipelinesx.config.ConfigUtil
import pipelinesx.config.ConfigUtil.implicits._
import pipelinesx.logging.{ Logger, LoggingUtil }
import scala.concurrent.duration._

/**
 * Reads wine records from a CSV file (which actually uses ";" as the separator),
 * parses it into a WineRecord and sends it downstream.
 */
final case object WineRecordIngress extends AkkaStreamlet {

  val out = AvroOutlet[WineRecord]("out", _.lot_id)

  final override val shape = StreamletShape(out)

  override final def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      WineRecordIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

object WineRecordIngressUtil {

  val rootConfigKey = "wine-quality"

  lazy val dataFrequencyMilliseconds: FiniteDuration =
    ConfigUtil.default
      .getOrElse[Int](rootConfigKey + ".data-frequency-milliseconds")(1).milliseconds

  def makeSource(
      configRoot: String         = rootConfigKey,
      frequency:  FiniteDuration = dataFrequencyMilliseconds): Source[WineRecord, NotUsed] = {
    val reader = makeRecordsReader(configRoot)
    Source.repeat(reader)
      .map(reader ⇒ reader.next()._2) // Only keep the record part of the tuple
      .throttle(1, frequency)
  }

  val defaultSeparator = ";"

  def makeRecordsReader(configRoot: String = rootConfigKey): RecordsReader[WineRecord] =
    RecordsReader.fromConfiguration[WineRecord](
      configurationKeyRoot = configRoot,
      dropFirstN = 0)(parse)

  val parse: String ⇒ Either[String, WineRecord] = line ⇒ {
    val tokens = line.split(defaultSeparator)
    if (tokens.length < 11) {
      Left(s"Record does not have 11 fields, ${tokens.mkString(defaultSeparator)}")
    } else try {
      val dtokens = tokens.map(_.trim.toDouble)
      Right(WineRecord(
        lot_id = "wine quality sample data",
        fixed_acidity = dtokens(0),
        volatile_acidity = dtokens(1),
        citric_acid = dtokens(2),
        residual_sugar = dtokens(3),
        chlorides = dtokens(4),
        free_sulfur_dioxide = dtokens(5),
        total_sulfur_dioxide = dtokens(6),
        density = dtokens(7),
        pH = dtokens(8),
        sulphates = dtokens(9),
        alcohol = dtokens(10)))
    } catch {
      case scala.util.control.NonFatal(nf) ⇒
        Left(
          s"Failed to parse string ${tokens.mkString(defaultSeparator)}. cause: $nf")
    }
  }

  val logger: Logger = LoggingUtil.getLogger(RecordsReader.getClass)
}

/**
 * Test program for [[WineRecordIngressUtil]]; reads wine records and prints them.
 */
object WineRecordIngressMain {

  /**
   * For testing purposes.
   * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
   * the console instead:
   * ```
   * import pipelines.examples.modelserving.winequality._
   * WineRecordIngressMain.main(Array("-n", "5"))
   * ```
   */
  def main(args: Array[String]): Unit = {
    val defaultN = 100
    val defaultF = WineRecordIngressUtil.dataFrequencyMilliseconds
    def help() = println(s"""
      |usage: WineRecordIngressMain [-h|--help] [-n|--count N] [-f|--frequency F]
      |where:
      |  -h | --help         print this message and exit
      |  -n | --count N      print N records and stop (default: $defaultN)
      |  -f | --frequency F  seconds between output records (default: $defaultF)
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
    implicit val system = ActorSystem("WineRecordIngressMain")
    implicit val mat = ActorMaterializer()
    val source = WineRecordIngressUtil.makeSource(
      WineRecordIngressUtil.rootConfigKey, frequency)
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

