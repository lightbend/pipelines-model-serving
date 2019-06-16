package pipelines.examples.ingestor

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelines.examples.data._
import pipelines.ingress.RecordsReader
import pipelines.util.ConfigUtil
import pipelines.util.ConfigUtil.implicits._
import scala.concurrent.duration._

/**
 * Reads wine records from a CSV file (which actually uses ";" as the separator),
 * parses it into a {@link WineRecord} and sends it downstream.
 */
class WineDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[WineRecord]("out", _.dataType)

  final override val shape = StreamletShape(out)

  protected lazy val dataFrequencySeconds =
    ConfigUtil.default
      .getOrElse[Int]("wine-quality.data-frequency-seconds")(1).seconds

  /** Public var to permit overrides in unit tests */
  var recordsResources: Seq[String] = WineDataIngress.WineQualityRecordsResources

  val recordsReader =
    RecordsReader.fromClasspath(WineDataIngress.WineQualityRecordsResources)(
      WineRecordsReader.csvParserWithSeparator(";"))

  val source: Source[WineRecord, NotUsed] =
    Source.repeat(NotUsed)
      .map(_ ⇒ recordsReader.next()._2) // Only keep the record part of the tuple
      .throttle(1, dataFrequencySeconds)

  override final def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph = source.to(atMostOnceSink(out))
  }
}

object WineDataIngress {

  lazy val WineQualityRecordsResources: Seq[String] =
    ConfigUtil.default.getOrFail[Seq[String]]("wine-quality.data-sources")

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("WineDataIngress-Main")
    implicit val mat = ActorMaterializer()
    val ingress = new WineDataIngress
    println(s"frequency (seconds): ${ingress.dataFrequencySeconds}")
    println(s"records sources:     ${WineDataIngress.WineQualityRecordsResources}")
    ingress.source.runWith(Sink.foreach(println))
  }
}

