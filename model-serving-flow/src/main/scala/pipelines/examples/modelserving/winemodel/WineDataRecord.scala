package pipelines.examples.modelserving.winemodel

import com.lightbend.modelserving.model.DataToServe
import pipelines.examples.data.WineRecord

case class WineDataRecord(record: WineRecord) extends DataToServe {
  def getType: String = record.dataType
  def getRecord: AnyVal = record.asInstanceOf[AnyVal]
}
