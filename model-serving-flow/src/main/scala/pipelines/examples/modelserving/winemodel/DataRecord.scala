package pipelines.examples.modelserving.winemodel

import com.lightbend.modelserving.model.DataToServe
import pipelines.examples.data.WineRecord

case class DataRecord(record: WineRecord) extends DataToServe[WineRecord] {
  def getType: String = record.dataType
  def getRecord: WineRecord = record
}
