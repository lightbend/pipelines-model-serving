package pipelines.examples.modelserving.speculative.model

import com.lightbend.modelserving.model.speculative.SpeculativeRecordSplitter
import org.apache.avro.specific.SpecificRecordBase
import pipelines.examples.modelserving.winequality.data.WineResult
import pipelines.examples.modelserving.winequality.speculative.WineResultRun

class WineSpeculativeRecordSplitter extends SpeculativeRecordSplitter[WineResult] {

  override def getUUID(source: SpecificRecordBase): String = source.asInstanceOf[WineResultRun].uuid

  override def getRecord(source: SpecificRecordBase): WineResult = source.asInstanceOf[WineResultRun].result
}
