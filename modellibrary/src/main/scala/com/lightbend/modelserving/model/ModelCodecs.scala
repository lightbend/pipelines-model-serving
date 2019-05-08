package com.lightbend.modelserving.model

import pipelines.examples.data.ModelDescriptor
import pipelines.streamlets.{ Keyed, KeyedSchema }
import pipelines.streamlets.avro.AvroKeyedSchema

class ModelCodecs {

  implicit val modelDescriptorKeyed: Keyed[ModelDescriptor] = (m: ModelDescriptor) => m.name
  implicit val modelDescriptorCodec: KeyedSchema[ModelDescriptor] = AvroKeyedSchema[ModelDescriptor](ModelDescriptor.SCHEMA$)
}

object ModelCodecs extends ModelCodecs {
  def instance: ModelCodecs = this
}
