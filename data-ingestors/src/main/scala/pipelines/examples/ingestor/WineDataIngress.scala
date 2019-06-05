package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

import scala.concurrent.duration._

/**
 * Reads wine records from a CSV file (which actually uses ; as the separator),
 * parses it into a {@link WineRecord} and sends it downstream.
 */
class WineDataIngress extends SourceIngress[WineRecord] {

  val recordsSource = scala.io.Source.fromResource("winequality_red.csv")
  val records = getListOfDataRecords()
  var recordsIterator = records.iterator

  override def createLogic = new SourceIngressLogic() {

    recordsIterator = records.iterator

    def source: Source[WineRecord, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ getWineRecord())
        .throttle(1, 1.seconds) // "dribble" them out
    }
  }

  def getWineRecord(): WineRecord = {
    recordsIterator.hasNext match {
      case false ⇒ recordsIterator = records.iterator // start over
      case _     ⇒
    }
    recordsIterator.next()
  }

  def getListOfDataRecords(): Seq[WineRecord] = {

    val result = recordsSource.getLines.foldLeft(
      Vector.empty[WineRecord]) {
        case (seq, line) ⇒
          val cols = line.split(";").map(_.trim.toDouble)
          if (cols.length < 11) {
            printf("ERROR: record does not have 11 fields after splitting string on ';': " + line)
            seq
          } else {
            val Array(
              fixed_acidity,
              volatile_acidity,
              citric_acid,
              residual_sugar,
              chlorides,
              free_sulfur_dioxide,
              total_sulfur_dioxide,
              density,
              pH,
              sulphates,
              alcohol) = cols.take(11)
            val record = WineRecord(
              fixed_acidity,
              volatile_acidity,
              citric_acid,
              residual_sugar,
              chlorides,
              free_sulfur_dioxide,
              total_sulfur_dioxide,
              density,
              pH,
              sulphates,
              alcohol,
              dataType = "wine")
            seq :+ record
          }
      }

    recordsSource.close
    result
  }
}

object WineDataIngress {
  def main(args: Array[String]): Unit = {
    val ingress = new WineDataIngress()
    while (true)
      println(ingress.getWineRecord())
  }
}
