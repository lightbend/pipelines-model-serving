package pipelinesx.egress.influxdb

import pipelines.streamlets.CodecInlet
import pipelines.akkastream._
import pipelines.akkastream.scaladsl._
import akka.actor.ActorSystem

/**
 * Egress logic abstraction for writing data to InfluxDB.
 * @param in The inlet for data to write to InfluxDB.
 * @param measurement The name of the measurement being written.
 * @param writer The object that knows how to write records of type IN.
 * @param configKeyRoot The application root context, under which the host, port, etc. are determined from the configuration.
 * @param configKeys The configuration keys used to retrieve the database host, port, and table name from the configuration, relative to the configKeyRoot.
 */
final case class InfluxDBEgressLogic[IN](
    in:            CodecInlet[IN],
    measurement:   String,
    writer:        InfluxDBUtil.Writer[IN],
    configKeyRoot: String,
    configKeys:    InfluxDBEgressLogic.ConfigKeys = InfluxDBEgressLogic.ConfigKeys())(
    implicit
    context: AkkaStreamletContext)
  extends RunnableGraphStreamletLogic {

  def runnableGraph =
    sourceWithOffsetContext(in)
      .via(flowWithContext(system).asFlow)
      .to(sinkWithOffsetContext)

  def flowWithContext(system: ActorSystem) = {
    val host = get(context, configKeyRoot + "." + configKeys.influxHost)
    val port = get(context, configKeyRoot + "." + configKeys.influxPort)
    val db = get(context, configKeyRoot + "." + configKeys.influxDatabase)

    val portInt =
      try { port.toInt }
      catch {
        case scala.util.control.NonFatal(th) ⇒
          throw InfluxDBEgressLogic.InvalidConfigValue(configKeyRoot + "." + configKeys.influxPort, port, th)
      }

    val influxDB = InfluxDBUtil.getInfluxDB(host, portInt)

    FlowWithOffsetContext[IN].map { record: IN ⇒
      system.log.debug(s"InfluxDBEgressLogic: to $measurement: $record")
      writer.write(record, measurement, db, influxDB)
      record
    }
  }

  protected def get(context: AkkaStreamletContext, key: String): String = try {
    val value = context.config.getString(key)
    if (value == null || value == "") throw InfluxDBEgressLogic.ConfigKeyNotFound(key, null)
    else value
  } catch {
    case scala.util.control.NonFatal(th) ⇒
      throw InfluxDBEgressLogic.ConfigKeyNotFound(key, th)
  }
}

object InfluxDBEgressLogic {

  final case class ConfigKeys(
      val influxHost:     String = "influxdb.host",
      val influxPort:     String = "influxdb.port",
      val influxDatabase: String = "influxdb.database")

  final case class ConfigKeyNotFound(key: String, cause: Throwable) extends RuntimeException(
    s"The InfluxDB key $key was not found. Please check your configuration, e.g., application.conf", cause)

  final case class InvalidConfigValue(key: String, value: String, cause: Throwable) extends RuntimeException(
    s"The InfluxDB value $value found for key $key was not valid. Please check your configuration, e.g., application.conf", cause)
}
