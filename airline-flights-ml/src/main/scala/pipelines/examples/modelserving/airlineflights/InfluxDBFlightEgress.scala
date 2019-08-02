package pipelines.examples.modelserving.airlineflights

import pipelines.examples.modelserving.airlineflights.data._
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelinesx.egress.influxdb.{ InfluxDBEgressLogic, InfluxDBUtil }
import org.influxdb.dto.Point

final case object InfluxDBAirlineFlightResultEgress extends AkkaStreamlet {
  val in = AvroInlet[AirlineFlightResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = new InfluxDBEgressLogic[AirlineFlightResult](
    in = in,
    configKeyRoot = "airline-flights",
    measurement = "airline_flight_result",
    writer = AirlineFlightResultInfluxDBWriter)
}

final case object InfluxDBAirlineFlightRecordEgress extends AkkaStreamlet {
  val in = AvroInlet[AirlineFlightRecord]("in")
  final override val shape = StreamletShape.withInlets(in)

  val writer: InfluxDBUtil.Writer[AirlineFlightRecord] = AirlineFlightRecordInfluxDBWriter

  override def createLogic = new InfluxDBEgressLogic[AirlineFlightRecord](
    in = in,
    configKeyRoot = "airline-flights",
    measurement = "airline_flight_record",
    writer = AirlineFlightRecordInfluxDBWriter)
}

object AirlineFlightResultInfluxDBWriter extends InfluxDBUtil.Writer[AirlineFlightResult] {
  def addFields(point: Point.Builder, record: AirlineFlightResult): Point.Builder = {
    point.addField("prediction_label", record.modelResult.label)
    point.addField("delay_probability", record.modelResult.probability)
    point
  }
}

object AirlineFlightRecordInfluxDBWriter extends InfluxDBUtil.Writer[AirlineFlightRecord] {
  def addFields(point: Point.Builder, record: AirlineFlightRecord): Point.Builder = {
    point.addField("carrier", record.uniqueCarrier)
    point.addField("destination", record.destination)
    point.addField("delay", record.arrDelay * 1.0)
    point
  }
}
