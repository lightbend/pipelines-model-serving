package pipelines.examples.modelserving.winequality.models

import com.lightbend.modelserving.model.DataToServe
import pipelines.examples.modelserving.winequality.data.WineRecord

case class WineDataRecord(record: WineRecord) extends DataToServe[WineRecord] {
  def getType: String = record.dataType
}
