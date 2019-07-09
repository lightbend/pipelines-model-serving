package pipelines.examples.ml.egress

import pipelines.akkastream.scaladsl.{ FlowEgress, FlowEgressLogic }
import pipelines.examples.data.AirlineFlightResult
import pipelines.examples.data.DataCodecs._

object InfluxDBFlightEgress extends FlowEgress[AirlineFlightResult] {

  // Config parameters
  val influxHost = "InfluxHost"
  val influxPort = "InfluxPort"
  val influxDatabase = "InfluxDatabase"
  override def configKeys = Set(influxHost, influxPort, influxDatabase)

  override def createLogic = new FlowEgressLogic() {

    val influxDB = InfluxDBUtil.getInfluxDB(streamletRefConfig.getString(influxHost), streamletRefConfig.getString(influxPort))

    def flow = flowWithPipelinesContext()
      .map { result ⇒
        println("InfluxDBEgress: result = " + result)
        InfluxDBUtil.write(result, "flight_ml_result", streamletRefConfig.getString(influxDatabase), influxDB)
        result
      }
  }
}
