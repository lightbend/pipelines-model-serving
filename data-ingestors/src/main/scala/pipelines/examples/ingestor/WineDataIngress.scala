package pipelines.examples.ingestor

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelines.examples.data.WineRecord
import pipelines.ingress.RecordsReader
import pipelines.config.ConfigUtil
import pipelines.config.ConfigUtil.implicits._
import scala.concurrent.duration._
import pipelines.logging.{ Logger, LoggingUtil }

/**
 * Reads wine records from a CSV file (which actually uses ";" as the separator),
 * parses it into a WineRecord and sends it downstream.
 */
final case object WineDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[WineRecord]("out", _.dataType)

  final override val shape = StreamletShape(out)

  override final def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      WineDataIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

object WineDataIngressUtil {

  val rootConfigKey = "wine-quality"

  lazy val dataFrequencyMilliseconds: FiniteDuration =
    ConfigUtil.default
      .getOrElse[Int](rootConfigKey + ".data-frequency-milliseconds")(1).milliseconds

  def makeSource(
      configRoot: String = rootConfigKey,
      frequency: FiniteDuration = dataFrequencyMilliseconds): Source[WineRecord, NotUsed] = {
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
        alcohol = dtokens(10),
        dataType = "wine"))
    } catch {
      case scala.util.control.NonFatal(nf) ⇒
        Left(
          s"Failed to parse string ${tokens.mkString(defaultSeparator)}. cause: $nf")
    }
  }

  val logger: Logger = LoggingUtil.getLogger(RecordsReader.getClass)
}

