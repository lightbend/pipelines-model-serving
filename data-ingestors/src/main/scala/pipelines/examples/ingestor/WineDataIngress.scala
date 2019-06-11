package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

import scala.concurrent.duration._

/**
 * Reads wine records from a CSV file (which actually uses ";" as the separator),
 * parses it into a {@link WineRecord} and sends it downstream.
 */
class WineDataIngress extends SourceIngress[WineRecord] {

  override def createLogic = new SourceIngressLogic() {

    val recordsReader =
      WineRecordsReader(WineDataIngress.WineQualityRecordsResources)

    def source: Source[WineRecord, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ recordsReader.next())
        .throttle(1, 1.seconds) // "dribble" them out
    }
  }
}

object WineDataIngress {

  val WineQualityRecordsResources: Seq[String] = Array("winequality_red.csv")
}

