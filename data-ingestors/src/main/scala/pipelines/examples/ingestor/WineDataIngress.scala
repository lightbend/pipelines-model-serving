package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

import scala.concurrent.duration._
import scala.io.BufferedSource

class WineDataIngress extends SourceIngress[WineRecord] {

  var records: Seq[WineRecord] = Seq.empty
  var recordsIterator: Iterator[WineRecord] = _

  override def createLogic = new SourceIngressLogic() {

    records = getListOfDataRecords(scala.io.Source.fromResource("winequality_red.csv"))
    recordsIterator = records.iterator

    def source: Source[WineRecord, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ getWineRecord())
        .throttle(1, 1.seconds) // "dribble" them out
    }
  }

  def getWineRecord(): WineRecord = {
    recordsIterator.hasNext match {
      case false ⇒ recordsIterator = records.iterator
      case _     ⇒
    }
    recordsIterator.next()
  }

  def getListOfDataRecords(source: BufferedSource): Seq[WineRecord] = {

    var result = Seq.empty[WineRecord]
    for (line ← source.getLines) {
      val cols = line.split(";").map(_.trim)
      val record = new WineRecord(
        fixed_acidity = cols(0).toDouble,
        volatile_acidity = cols(1).toDouble,
        citric_acid = cols(2).toDouble,
        residual_sugar = cols(3).toDouble,
        chlorides = cols(4).toDouble,
        free_sulfur_dioxide = cols(5).toDouble,
        total_sulfur_dioxide = cols(6).toDouble,
        density = cols(7).toDouble,
        pH = cols(8).toDouble,
        sulphates = cols(9).toDouble,
        alcohol = cols(10).toDouble,
        dataType = "wine",
        ts = System.currentTimeMillis()
      )
      result = record +: result
    }
    source.close
    result
  }
}
