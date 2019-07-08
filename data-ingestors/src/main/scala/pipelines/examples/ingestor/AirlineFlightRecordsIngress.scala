package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

import scala.concurrent.duration._
import scala.collection.JavaConverters._

/**
 * Load Airline flight data at a rate specified in the application configuration.
 */
class AirlineFlightRecordsIngress extends SourceIngress[AirlineFlightRecord] {

  protected lazy val dataFrequencySeconds =
    this.context.config.getInt("airline-flights.data-frequency-seconds")

  override def createLogic = new SourceIngressLogic() {

    val recordsReader =
      CSVReader[AirlineFlightRecord](
        resourceNames = AirlineFlightRecordsIngress.RecordsResources,
        separator = ",",
        dropFirstLines = 1)(AirlineFlightRecordsIngress.parse)

    def source: Source[AirlineFlightRecord, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ recordsReader.next())
        .throttle(1, dataFrequencySeconds.seconds)
    }
  }
}

object AirlineFlightRecordsIngress {

  protected lazy val config = com.typesafe.config.ConfigFactory.load()
  lazy val RecordsResources: Seq[String] =
    config.getStringList("airline-flights.data-sources").asScala

  def parse(tokens: Array[String]): Either[String, AirlineFlightRecord] = {
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
      CSVReader[AirlineFlightRecord](
        resourceNames = AirlineFlightRecordsIngress.RecordsResources,
        separator = ",",
        dropFirstLines = 1)(AirlineFlightRecordsIngress.parse)
    (1 to count).foreach { n ⇒
      val record = reader.next()
      println("%7d: %s".format(n, record))
    }
  }
}
