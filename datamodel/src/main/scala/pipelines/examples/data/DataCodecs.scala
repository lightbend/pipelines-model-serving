package pipelines.examples.data

import pipelines.streamlets.avro._
import pipelines.streamlets._

class DataCodecs {
  implicit val wineRecordKeyed: Keyed[WineRecord] = (m: WineRecord) => m.dataType
  implicit val wineRecordCodec: KeyedSchema[WineRecord] = AvroKeyedSchema[WineRecord](WineRecord.SCHEMA$)

  implicit val resultKeyed: Keyed[Result] = (r: Result) => r.dataType
  implicit val resultCodec: KeyedSchema[Result] = AvroKeyedSchema[Result](Result.SCHEMA$)
}

object DataCodecs extends DataCodecs {
  def instance: DataCodecs = this
}
