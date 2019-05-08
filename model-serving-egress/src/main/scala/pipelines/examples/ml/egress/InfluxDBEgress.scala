package pipelines.examples.ml.egress

import pipelines.akkastream.scaladsl.{ FlowEgress, FlowEgressLogic }
import pipelines.examples.data._
import pipelines.examples.data.DataCodecs._

object InfluxDBEgress extends FlowEgress[Result] {
  override def createLogic = new FlowEgressLogic() {

    //    val influxHost = context.streamletRefConfig.getString("influxdb-hostname")
    //    val influxPort = Try(context.streamletRefConfig.getString("influxdb-port")).getOrElse("8086")
    //
    //    val influxDBDatabase = Try(context.streamletRefConfig.getString("influxdb-database")).getOrElse("wine-ml")

    val influxHost = "influxdb.influxdb.svc"
    val influxPort = "8086"

    val influxDBDatabase = "wine_ml"

    val influxDB = InfluxDBUtil.getInfluxDB(influxHost, influxPort)

    def flow = contextPropagatedFlow()
      .map { result ⇒
        {
          result.result match {
            case Some(value) ⇒
              println("Result: " + value)
              InfluxDBUtil.write(result, "wine_result", influxDBDatabase, influxDB)
            case _ ⇒
          }
          result
        }
      }
  }
}
