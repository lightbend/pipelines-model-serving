package pipelines.examples.ml.egress

import pipelines.akkastream.scaladsl.{ FlowEgress, FlowEgressLogic }
import pipelines.examples.data.Codecs._
import pipelines.examples.data._

object InfluxDBRawEgress extends FlowEgress[WineRecord] {
  override def createLogic = new FlowEgressLogic() {

    //    val influxHost = context.streamletRefConfig.getString("influxdb-hostname")
    //    val influxPort = Try(context.streamletRefConfig.getString("influxdb-port")).getOrElse("8086")
    //
    //    val influxDBDatabase = Try(context.streamletRefConfig.getString("influxdb-database")).getOrElse("wine-ml")

    val influxHost = "influxdb.killrweather.svc"
    val influxPort = "8086"

    val influxDBDatabase = "wine_ml"

    val influxDB = InfluxDBUtil.getInfluxDB(influxHost, influxPort)

    def flow = contextPropagatedFlow()
      .map { record ⇒
        {
          InfluxDBUtil.write(record, "wine_record", influxDBDatabase, influxDB)
          record
        }
      }
  }
}
