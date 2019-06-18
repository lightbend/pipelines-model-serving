package pipelines.egress.influxdb

import pipelines.egress.FlowEgress
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.scaladsl._
import akka.actor.ActorSystem
import scala.reflect.ClassTag
import org.apache.avro.specific.SpecificRecordBase

/**
 * Egress abstraction for writing data to InfluxDB.
 * @param measurement The name of the measurement being written.
 * @param configKeys the database host, port, etc. are read from the configuration.
 */
abstract class InfluxDBEgress(
  val measurement: String,
  val configKeys: InfluxDBEgress.ConfigKeys = InfluxDBEgress.ConfigKeys()) extends FlowEgress {

  val writer: InfluxDBUtil.Writer[IN]

  //override def configKeys = Set(configKeys.influxHost, configKeys.influxPort, configKeys.influxDatabase)

  def flowWithContext(system: ActorSystem) = {
    val influxDB = InfluxDBUtil.getInfluxDB(
      context.streamletRefConfig.getString(configKeys.influxHost),
      context.streamletRefConfig.getString(configKeys.influxPort))

    FlowWithPipelinesContext[IN].map { record: IN ⇒
      system.log.debug(s"InfluxDBEgress: to $measurement: $record")
      writer.write(record, measurement,
        context.streamletRefConfig.getString(configKeys.influxDatabase), influxDB)
      record
    }
  }
}

object InfluxDBEgress {
  final case class ConfigKeys(
    val influxHost: String = "InfluxHost",
    val influxPort: String = "InfluxPort",
    val influxDatabase: String = "InfluxDatabase")
}
