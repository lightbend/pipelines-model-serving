package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

import scala.concurrent.duration._
import scala.collection.JavaConverters._

/**
 * Reads wine records from a CSV file (which actually uses ";" as the separator),
 * parses it into a {@link WineRecord} and sends it downstream.
 */
class WineDataIngress extends SourceIngress[WineRecord] {

  /** Public var to permit overrides in unit tests */
  var recordsResources: Seq[String] = WineDataIngress.WineQualityRecordsResources

  protected lazy val dataFrequencyMilliseconds =
    this.context.config.getInt("wine-quality.data-frequency-milliseconds")

  override def createLogic = new SourceIngressLogic() {

    val recordsReader =
      WineRecordsReader(WineDataIngress.WineQualityRecordsResources)

    def source: Source[WineRecord, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ recordsReader.next())
        .throttle(1, dataFrequencyMilliseconds.milliseconds)
    }
  }
}

object WineDataIngress {

  protected lazy val config = com.typesafe.config.ConfigFactory.load()

  lazy val WineQualityRecordsResources: Seq[String] =
    config.getStringList("wine-quality.data-sources").asScala

  def main(args: Array[String]): Unit = {
    val wdi = new WineDataIngress
    println(wdi.dataFrequencyMilliseconds)
    println(WineDataIngress.WineQualityRecordsResources)
  }
}

