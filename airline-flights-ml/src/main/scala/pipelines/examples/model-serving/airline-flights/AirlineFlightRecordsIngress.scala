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

  val out = AvroOutlet[AirlineFlightRecord](
    "out",
    r ⇒ s"${r.year}-${r.month}-${r.dayOfMonth}-${r.depTime}-${r.uniqueCarrier}-${r.flightNum}")

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
    configRoot: String = rootConfigKey,
    frequency: FiniteDuration = dataFrequencyMilliseconds): Source[AirlineFlightRecord, NotUsed] = {
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

  def main(args: Array[String]): Unit = {
    val count = if (args.length > 0) args(0).toInt else 100000

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
