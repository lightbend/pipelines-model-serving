package pipelines.egress.influxdb

import pipelines.egress.FlowEgressLogic
import pipelines.streamlets.CodecInlet
import pipelines.akkastream.StreamletContext
import pipelines.akkastream.scaladsl._
import akka.actor.ActorSystem
import scala.reflect.ClassTag
import org.apache.avro.specific.SpecificRecordBase

/**
 * Egress logic abstraction for writing data to InfluxDB.
 * @param measurement The name of the measurement being written.
 * @param configKeys the database host, port, etc. are read from the configuration.
 */
final case class InfluxDBEgressLogic[IN](
  in: CodecInlet[IN],
  measurement: String,
  writer: InfluxDBUtil.Writer[IN],
  configKeys: InfluxDBEgressLogic.ConfigKeys = InfluxDBEgressLogic.ConfigKeys())(
  implicit
  context: StreamletContext)
  extends FlowEgressLogic[IN](in) {

  def flowWithContext(system: ActorSystem) = {
    val influxDB = InfluxDBUtil.getInfluxDB(
      context.streamletConfig.getString(configKeys.influxHost),
      context.streamletConfig.getString(configKeys.influxPort))

    FlowWithPipelinesContext[IN].map { record: IN ⇒
      system.log.debug(s"InfluxDBEgressLogic: to $measurement: $record")
      writer.write(record, measurement,
        context.streamletConfig.getString(configKeys.influxDatabase), influxDB)
      record
    }
  }
}

object InfluxDBEgressLogic {
  final case class ConfigKeys(
    val influxHost: String = "InfluxHost",
    val influxPort: String = "InfluxPort",
    val influxDatabase: String = "InfluxDatabase")
}
