
package pipelines.examples.modelserving.winequality

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.streamlets.StreamletShape
import pipelinesx.ingress.RecordsReader
import pipelinesx.config.ConfigUtil
import pipelinesx.config.ConfigUtil.implicits._
import pipelinesx.logging.{ Logger, LoggingUtil }

import scala.concurrent.duration._
import com.lightbend.modelserving.model.util.MainBase
import pipelines.examples.modelserving.winequality.data.WineRecord
import pipelines.streamlets.avro.AvroOutlet

/**
 * Reads wine records from a CSV file (which actually uses ";" as the separator),
 * parses it into a WineRecord and sends it downstream.
 */
final case object WineRecordIngress extends AkkaStreamlet {

  val out = AvroOutlet[WineRecord]("out", _.lot_id)

  final override val shape = StreamletShape(out)

  override final def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      WineRecordIngressUtil.makeSource().to(plainSink(out))
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
 * Test program for [[WineRecordIngress]] and [[WineRecordIngressUtil]];
 * reads records and prints them. For testing purposes only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.winequality._
 * WineRecordIngressMain.main(Array("-n","10","-f","1000"))
 * ```
 */
object WineRecordIngressMain extends MainBase[WineRecord](
  defaultCount = 10,
  defaultFrequencyMillis = WineRecordIngressUtil.dataFrequencyMilliseconds) {

  override protected def makeSource(frequency: FiniteDuration): Source[WineRecord, NotUsed] =
    WineRecordIngressUtil.makeSource(
      WineRecordIngressUtil.rootConfigKey, frequency)
}
