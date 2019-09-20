package pipelines.examples.modelserving.winequality

import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelinesx.egress.influxdb.{ InfluxDBEgressLogic, InfluxDBUtil }
import org.influxdb.dto.Point
import pipelines.examples.modelserving.winequality.data.{ WineRecord, WineResult }

final case object InfluxDBWineResultEgress extends AkkaStreamlet {
  val in = AvroInlet[WineResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = new InfluxDBEgressLogic[WineResult](
    in = in,
    configKeyRoot = "wine-quality",
    measurement = "wine_result",
    writer = WineResultInfluxDBWriter)
}

final case object InfluxDBWineRecordEgress extends AkkaStreamlet {
  val in = AvroInlet[WineRecord]("in")
  final override val shape = StreamletShape.withInlets(in)

  val writer: InfluxDBUtil.Writer[WineRecord] = WineRecordInfluxDBWriter

  override def createLogic = new InfluxDBEgressLogic[WineRecord](
    in = in,
    configKeyRoot = "wine-quality",
    measurement = "wine_record",
    writer = WineRecordInfluxDBWriter)
}

object WineResultInfluxDBWriter extends InfluxDBUtil.Writer[WineResult] {
  def addFields(point: Point.Builder, record: WineResult): Point.Builder = {
    point.addField("result", record.modelResult.value)
    point.addField("duration", record.modelResultMetadata.duration)
    point.tag("model", record.modelResultMetadata.modelName)
    point
  }
}

object WineRecordInfluxDBWriter extends InfluxDBUtil.Writer[WineRecord] {
  def addFields(point: Point.Builder, record: WineRecord): Point.Builder = {
    point.addField("alcohol", record.alcohol)
    point.addField("ph", record.pH)
    point.addField("citric_acid", record.citric_acid)
    point
  }
}

