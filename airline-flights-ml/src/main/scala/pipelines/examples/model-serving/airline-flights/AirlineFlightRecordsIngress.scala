package pipelines.examples.modelserving.airlineflights

import pipelines.examples.modelserving.airlineflights.data._
import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import pipelines.akkastream.AkkaStreamlet
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelinesx.ingress.RecordsReader
import pipelinesx.config.ConfigUtil
import pipelinesx.config.ConfigUtil.implicits._
import scala.concurrent.duration._

/**
 * Load Airline flight data at a rate specified in the application configuration.
 */
final case object AirlineFlightRecordsIngress extends AkkaStreamlet {

  // Use ONE partition for input to model serving
  val out = AvroOutlet[AirlineFlightRecord]("out", _ ⇒ "airlines")

  final override val shape = StreamletShape(out)

  override final def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      AirlineFlightRecordsIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

object AirlineFlightRecordsIngressUtil {

  val rootConfigKey = "airline-flights"

  lazy val dataFrequencyMilliseconds: FiniteDuration =
    ConfigUtil.default
      .getOrElse[Int](rootConfigKey + ".data-frequency-milliseconds")(1).milliseconds

  def makeSource(
      configRoot: String         = rootConfigKey,
      frequency:  FiniteDuration = dataFrequencyMilliseconds): Source[AirlineFlightRecord, NotUsed] = {
    val reader = makeRecordsReader(configRoot)
    Source.repeat(NotUsed)
      .map(_ ⇒ reader.next()._2) // Only keep the record part of the tuple
      .throttle(1, frequency)
  }

  def makeRecordsReader(configRoot: String = rootConfigKey): RecordsReader[AirlineFlightRecord] =
    RecordsReader.fromConfiguration[AirlineFlightRecord](
      configurationKeyRoot = configRoot,
      dropFirstN = 1)(parse)

  val parse: String ⇒ Either[String, AirlineFlightRecord] = line ⇒ {
    val tokens = line.split(",")
    if (tokens.length < 29) {
      Left("ERROR: record does not have 29 fields.")
    } else try {
      val dtokens = tokens.map { tok ⇒
        val tok2 = tok.trim
        if (tok2 == "NA") "0" else tok2 // handles "NA" values in data
      }
      Right(AirlineFlightRecord(
        year = dtokens(0).toInt,
        month = dtokens(1).toInt,
        dayOfMonth = dtokens(2).toInt,
        dayOfWeek = dtokens(3).toInt,
        depTime = dtokens(4).toInt,
        crsDepTime = dtokens(5).toInt,
        arrTime = dtokens(6).toInt,
        crsArrTime = dtokens(7).toInt,
        uniqueCarrier = dtokens(8),
        flightNum = dtokens(9).toInt,
        tailNum = dtokens(10).toInt,
        actualElapsedTime = dtokens(11).toInt,
        crsElapsedTime = dtokens(12).toInt,
        airTime = dtokens(13).toInt,
        arrDelay = dtokens(14).toInt,
        depDelay = dtokens(15).toInt,
        origin = dtokens(16),
        destination = dtokens(17),
        distance = dtokens(18).toInt,
        taxiIn = dtokens(19).toInt,
        taxiOut = dtokens(20).toInt,
        canceled = dtokens(21).toInt,
        cancellationCode = dtokens(22).toInt,
        diverted = dtokens(23).toInt,
        carrierDelay = dtokens(24).toInt,
        weatherDelay = dtokens(25).toInt,
        nASDelay = dtokens(26).toInt,
        securityDelay = dtokens(27).toInt,
        lateAircraftDelay = dtokens(28).toInt))
    } catch {
      case scala.util.control.NonFatal(nf) ⇒
        Left(
          s"ERROR: Failed to parse string ${tokens.mkString(",")}. cause: $nf")
    }
  }

  /**
   * WARNING: Currently, the Pipelines plugin interferes with running mains,
   * even when you use
   *   runMain pipelines.examples.modelserving.airlineflights.AirlineFlightModelServerMain
   * Instead, start the console and run it there:
   * ```
   * import pipelines.examples.modelserving.airlineflights._
   * AirlineFlightRecordsIngressUtil.main(Array("-n", "10"))
   * ```
   */
  def main(args: Array[String]): Unit = {
    val defaultN = 1000
      def parseArgs(args2: Seq[String], count: Int): Int = args2 match {
        case Nil ⇒ count
        case ("-h" | "--help") +: _ ⇒
          help()
          sys.exit(0)
        case ("-n" | "--count") +: x +: tail ⇒ parseArgs(tail, x.toInt)
        case x +: _ ⇒
          println(s"ERROR: Invalid argument $x. (args = ${args.mkString(" ")}")
          help()
          sys.exit(1)
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

    val count = parseArgs(args, defaultN)
    val reader =
      RecordsReader.fromConfiguration[AirlineFlightRecord](
        configurationKeyRoot = rootConfigKey,
        dropFirstN = 1)(parse)

    (1 to count).foreach { n ⇒
      val record = reader.next()
      println("%7d: %s".format(n, record))
    }
  }
}
