package com.lightbend.modelserving.model.speculative

import org.apache.avro.specific.SpecificRecordBase

abstract class SpeculativeRecordSplitter[Output] {
  def getUUID(source: SpecificRecordBase): String
  def getRecord(source: SpecificRecordBase): Output
}
