package pipelines.examples.ml.egress

import pipelines.akkastream.scaladsl.{ FlowEgress, FlowEgressLogic }
import pipelines.examples.data._
import pipelines.examples.data.DataCodecs._

object InfluxDBEgress extends FlowEgress[WineResult] {

  // Config parameters
  val influxHost = "InfluxHost"
  val influxPort = "InfluxPort"
  val influxDatabase = "InfluxDatabase"
  override def configKeys = Set(influxHost, influxPort, influxDatabase)

  override def createLogic = new FlowEgressLogic() {

    val influxDB = InfluxDBUtil.getInfluxDB(streamletRefConfig.getString(influxHost), streamletRefConfig.getString(influxPort))

    def flow = contextPropagatedFlow()
      .map { result ⇒
        {
          result.result match {
            case Some(value) ⇒
              println("Result: " + value)
              InfluxDBUtil.write(result, "wine_result", streamletRefConfig.getString(influxDatabase), influxDB)
            case _ ⇒
          }
          result
        }
      }
  }
}
