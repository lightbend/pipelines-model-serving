package pipelines.examples.ingestor

import pipelines.akkastream.scaladsl._
import pipelines.examples.data._
import pipelines.examples.data.Codecs._

class WineDataParser extends Splitter[IndexedCSV, InvalidRecord, WineRecord] {

  override def createLogic = new SplitterLogic() {
    def flow = contextPropagatedFlow()
      .map {
        case IndexedCSV(index, csv) ⇒ {
          WineRecordUtil.toWineRecord(csv.split(";")) match {
            case Left(error)   ⇒ Left(InvalidRecord(s"$index: $csv", error))
            case Right(record) ⇒ Right(record)
          }
        }
      }
  }
}

