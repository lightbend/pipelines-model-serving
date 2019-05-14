package pipelines.examples.ml.egress

import pipelines.akkastream.scaladsl.{ FlowEgress, FlowEgressLogic }
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

object InfluxDBRawEgress extends FlowEgress[WineRecord] {

  // Config parameters
  val influxHost = "InfluxHost"
  val influxPort = "InfluxPort"
  val influxDatabase = "InfluxDatabase"
  override def configKeys = Set(influxHost, influxPort, influxDatabase)

  override def createLogic = new FlowEgressLogic() {

    val influxDB = InfluxDBUtil.getInfluxDB(streamletRefConfig.getString(influxHost), streamletRefConfig.getString(influxPort))

    def flow = contextPropagatedFlow()
      .map { record ⇒
        {
          println(s"Writing Record: ${record.dataType}")
          InfluxDBUtil.write(record, "wine_record", streamletRefConfig.getString(influxDatabase), influxDB)
          record
        }
      }
  }
}
