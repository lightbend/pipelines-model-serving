package pipelines.examples.modelserving.airlineflightmodel

import com.lightbend.modelserving.model.DataToServe
import pipelines.examples.data.AirlineFlightRecord

case class AirlineFlightDataRecord(record: AirlineFlightRecord) extends DataToServe {
  def getType: String = record.dataType
  def getRecord: AnyVal = record.asInstanceOf[AnyVal]
}
