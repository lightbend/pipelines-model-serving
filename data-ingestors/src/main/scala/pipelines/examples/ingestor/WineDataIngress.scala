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

  override final def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph = source().to(atMostOnceSink(out))
  }

  protected def source(): Source[WineRecord, NotUsed] = {
    val reader = WineDataIngress.recordsReader()
    Source.repeat(NotUsed)
      .map(_ ⇒ reader.next()._2) // Only keep the record part of the tuple
      .throttle(1, WineDataIngress.dataFrequencySeconds)
  }
}

object WineDataIngress {

  lazy val dataFrequencySeconds: FiniteDuration =
    ConfigUtil.default
      .getOrElse[Int]("wine-quality.data-frequency-seconds")(1).seconds

  lazy val WineQualityRecordsResources: Seq[String] =
    ConfigUtil.default.getOrFail[Seq[String]]("wine-quality.data-sources")

  def recordsReader(): RecordsReader[WineRecord] =
    RecordsReader.fromClasspath(WineQualityRecordsResources)(
      WineRecordsReader.csvParserWithSeparator(";"))

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("WineDataIngress-Main")
    implicit val mat = ActorMaterializer()
    val ingress = new WineDataIngress
    println(s"frequency (seconds): ${dataFrequencySeconds}")
    println(s"records sources:     ${WineQualityRecordsResources}")
    ingress.source().runWith(Sink.foreach(println))
  }
}

