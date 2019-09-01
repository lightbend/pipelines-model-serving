package pipelines.examples.modelserving.airlineflights

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import pipelines.akkastream.AkkaStreamlet
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelinesx.reader.RecordsReader
import net.ceedubs.ficus.Ficus._
import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.duration._
import pipelinesx.modelserving.model.util.MainBase
import pipelines.examples.modelserving.airlineflights.data.AirlineFlightRecord

/**
 * Load Airline flight data at a rate specified in the application configuration.
 */
final case object AirlineFlightRecordIngress extends AkkaStreamlet {

  // Use ONE partition for input to model serving
  val out = AvroOutlet[AirlineFlightRecord]("out", _ ⇒ "airlines")

  final override val shape = StreamletShape(out)

  override final def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      AirlineFlightRecordIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

object AirlineFlightRecordIngressUtil {

  val rootConfigKey = "airline-flights"

  private val config: Config = ConfigFactory.load()

  lazy val dataFrequencyMilliseconds: FiniteDuration =
    config.as[Option[Int]](rootConfigKey + ".data-frequency-milliseconds").getOrElse(1).milliseconds

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
}

/**
 * Test program for [[AirlineFlightRecordIngress]] and [[AirlineFlightRecordIngressUtil]];
 * reads records and prints them. For testing purposes only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.airlineflights._
 * AirlineFlightRecordIngressMain.main(Array("-n","10","-f","1000"))
 * ```
 */
object AirlineFlightRecordIngressMain extends MainBase[AirlineFlightRecord](
  defaultCount = 10,
  defaultFrequencyMillis = AirlineFlightRecordIngressUtil.dataFrequencyMilliseconds) {

  override protected def makeSource(frequency: FiniteDuration): Source[AirlineFlightRecord, NotUsed] =
    AirlineFlightRecordIngressUtil.makeSource(
      AirlineFlightRecordIngressUtil.rootConfigKey, frequency)
}
