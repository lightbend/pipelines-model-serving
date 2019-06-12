package pipelines.examples.util.test

import pipelines.streamlets.avro._
import pipelines.streamlets._

class TestDataCodecs {
  implicit val dataRecordKeyed: Keyed[TestData] = (m: TestData) => m.name
  implicit val dataRecordCodec: KeyedSchema[TestData] = AvroKeyedSchema[TestData](TestData.SCHEMA$)
}

object TestDataCodecs extends TestDataCodecs {
  def instance: TestDataCodecs = this
}

