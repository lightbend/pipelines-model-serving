package pipelines.examples.main

import pipelines.examples.modelserving.AirlineFlightModelServer
import pipelines.examples.ingestor.{ AirlineFlightRecordsIngress, CSVReader }
import pipelines.examples.data._
import pipelines.examples.data.DataCodecs._

object Main {
  def main(args: Array[String]): Unit = {
    val count = if (args.length > 0) args(0).toInt else 1000
    val server = new AirlineFlightModelServer()
    val reader =
      CSVReader[AirlineFlightRecord](
        resourceNames = AirlineFlightRecordsIngress.RecordsResources,
        separator = ",",
        dropFirstLines = 1)(AirlineFlightRecordsIngress.parse)
    (1 to count).foreach { n ⇒
      val record = reader.next()
      val result = server.score(record)
      println("%7d: %s".format(n, result))
      Thread.sleep(100)
    }
  }
}
