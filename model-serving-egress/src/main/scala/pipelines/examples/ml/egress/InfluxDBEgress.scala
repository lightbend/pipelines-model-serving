package pipelines.examples.ml.egress

import pipelines.streamlets.avro._
import pipelines.akkastream.scaladsl._
import akka.actor.ActorSystem
import scala.reflect.ClassTag
import org.apache.avro.specific.SpecificRecordBase

/**
 * Egress abstraction for writing data to InfluxDB.
 * @param measurement The name of the measurement being written.
 * @param configKeys the database host, port, etc. are read from the configuration.
 */
abstract class InfluxDBEgress[R <: SpecificRecordBase: ClassTag](
    val measurement: String,
    val configKeys: InfluxDBEgress.ConfigKeys = InfluxDBEgress.ConfigKeys())
  extends FlowEgress[R](AvroInlet[R]("in")) {

  val writer: InfluxDBUtil.Writer[R]

  //override def configKeys = Set(configKeys.influxHost, configKeys.influxPort, configKeys.influxDatabase)

  def flowWithContext(system: ActorSystem) = {
    val influxDB = InfluxDBUtil.getInfluxDB(
      context.streamletRefConfig.getString(configKeys.influxHost),
      context.streamletRefConfig.getString(configKeys.influxPort))

    FlowWithPipelinesContext[R].map { record: R ⇒
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
