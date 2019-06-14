package pipelines.examples.ingestor

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro._
import pipelines.akkastream.{ AkkaStreamlet, StreamletLogic }
import pipelines.examples.data._
import pipelines.examples.util.ConfigUtil
import pipelines.examples.util.ConfigUtil.implicits._
import scala.concurrent.duration._

/**
 * Reads wine records from a CSV file (which actually uses ";" as the separator),
 * parses it into a {@link WineRecord} and sends it downstream.
 */
class WineDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[WineRecord]("out", _.dataType)

  final override val shape = StreamletShape(out)

  protected lazy val dataFrequencySeconds =
    ConfigUtil(this.context.config)
      .getOrElse[Int]("wine-quality.data-frequency-seconds")(1).seconds

  /** Public var to permit overrides in unit tests */
  var recordsResources: Seq[String] = WineDataIngress.WineQualityRecordsResources

  val recordsReader =
    WineRecordsReader(WineDataIngress.WineQualityRecordsResources)

  val source: Source[WineRecord, NotUsed] =
    Source.repeat(NotUsed)
      .map(_ ⇒ recordsReader.next())
      .throttle(1, dataFrequencySeconds)

  override def createLogic = new StreamletLogic() {
    def run(): Unit = source.to(atLeastOnceSink(out))
  }
}

object WineDataIngress {

  lazy val WineQualityRecordsResources: Seq[String] =
    ConfigUtil.default.getOrFail[Seq[String]]("wine-quality.data-sources")

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("WineDataIngress - Main")
    implicit val mat = ActorMaterializer()
    val ingress = new WineDataIngress
    println(s"frequency (seconds): ${ingress.dataFrequencySeconds}")
    println(s"records sources:     ${WineDataIngress.WineQualityRecordsResources}")
    ingress.source.runWith(Sink.foreach(println))
  }
}

