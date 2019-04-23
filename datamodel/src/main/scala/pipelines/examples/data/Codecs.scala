package pipelines.examples.data

import pipelines.streamlets.avro._
import pipelines.streamlets._

class Codecs {
  implicit val IndexedCSVKeyed: Keyed[IndexedCSV] = (t: IndexedCSV) => t.index.toString
  implicit val IndexedCSVCodec: KeyedSchema[IndexedCSV] = AvroKeyedSchema[IndexedCSV](IndexedCSV.SCHEMA$)

  implicit val modelDescriptorKeyed: Keyed[ModelDescriptor] = (m: ModelDescriptor) => m.name
  implicit val modelDescriptorCodec: KeyedSchema[ModelDescriptor] = AvroKeyedSchema[ModelDescriptor](ModelDescriptor.SCHEMA$)

  implicit val wineRecordKeyed: Keyed[WineRecord] = (m: WineRecord) => m.model_name
  implicit val wineRecordCodec: KeyedSchema[WineRecord] = AvroKeyedSchema[WineRecord](WineRecord.SCHEMA$)

  implicit val invalidRecordKeyed: Keyed[InvalidRecord] = (r: InvalidRecord) => r.record
  implicit val invalidRecordCodec: KeyedSchema[InvalidRecord] = AvroKeyedSchema[InvalidRecord](InvalidRecord.SCHEMA$)

  implicit val mlActionKeyed: Keyed[MlAction] = (r: MlAction) => r.action
  implicit val mlActionCodec: KeyedSchema[MlAction] = AvroKeyedSchema[MlAction](MlAction.SCHEMA$)

  implicit val resultKeyed: Keyed[Result] = (r: Result) => r.dataType
  implicit val resultCodec: KeyedSchema[Result] = AvroKeyedSchema[Result](Result.SCHEMA$)

  implicit val modelUpdateConfirmKeyed: Keyed[ModelUpdateConfirm] = (m: ModelUpdateConfirm) => m.modeltype
  implicit val modelUpdateConfirmCodec: KeyedSchema[ModelUpdateConfirm] = AvroKeyedSchema[ModelUpdateConfirm](ModelUpdateConfirm.SCHEMA$)
}

object Codecs extends Codecs {
  def instance: Codecs = this
}
