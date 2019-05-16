package pipelines.examples.data

import pipelines.streamlets.avro._
import pipelines.streamlets._

class DataCodecs {
  implicit val wineRecordKeyed: Keyed[WineRecord] = (m: WineRecord) => m.dataType
  implicit val wineRecordCodec: KeyedSchema[WineRecord] = AvroKeyedSchema[WineRecord](WineRecord.SCHEMA$)

  implicit val wineresultKeyed: Keyed[WineResult] = (r: WineResult) => r.dataType
  implicit val wineresultCodec: KeyedSchema[WineResult] = AvroKeyedSchema[WineResult](WineResult.SCHEMA$)

  implicit val recommenderRecordKeyed: Keyed[RecommenderRecord] = (m: RecommenderRecord) => m.dataType
  implicit val recommenderRecordCodec: KeyedSchema[RecommenderRecord] = AvroKeyedSchema[RecommenderRecord](RecommenderRecord.SCHEMA$)

  implicit val recommenderResultKeyed: Keyed[RecommendationResult] = (r: RecommendationResult) => r.dataType
  implicit val recommenderResultCodec: KeyedSchema[RecommendationResult] = AvroKeyedSchema[RecommendationResult](RecommendationResult.SCHEMA$)
}

object DataCodecs extends DataCodecs {
  def instance: DataCodecs = this
}
