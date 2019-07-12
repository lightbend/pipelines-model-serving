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
import pipelines.ingress.RecordsFilesReader
import pipelines.util.ConfigUtil
import pipelines.util.ConfigUtil.implicits._
import scala.concurrent.duration._

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

  lazy val dataFrequencyMilliseconds: FiniteDuration =
    ConfigUtil.default
      .getOrElse[Int]("wine-quality.data-frequency-milliseconds")(1).milliseconds

  lazy val wineQualityRecordsResources: Seq[String] =
    ConfigUtil.default.getOrFail[Seq[String]]("wine-quality.data-sources")

  def makeSource(
      recordsResources: Seq[String] = wineQualityRecordsResources,
      frequency: FiniteDuration = dataFrequencyMilliseconds): Source[WineRecord, NotUsed] = {
    val reader = makeRecordsFilesReader(recordsResources)
    Source.repeat(reader)
      .map(reader ⇒ reader.next()._2) // Only keep the record part of the tuple
      .throttle(1, frequency)
  }

  def makeRecordsFilesReader(sources: Seq[String] = wineQualityRecordsResources): RecordsFilesReader[WineRecord] =
    RecordsFilesReader.fromClasspath(sources)(
      WineRecordsFilesReader.csvParserWithSeparator(";"))

  /** For testing purposes. */
  def main(args: Array[String]): Unit = {
    println(s"frequency (seconds): ${dataFrequencyMilliseconds}")
    println(s"records sources:     ${wineQualityRecordsResources}")
    implicit val system = ActorSystem("RecommenderDataIngress-Main")
    implicit val mat = ActorMaterializer()
    val source = makeSource(wineQualityRecordsResources, dataFrequencyMilliseconds)
    source.runWith(Sink.foreach(println))
  }
}

