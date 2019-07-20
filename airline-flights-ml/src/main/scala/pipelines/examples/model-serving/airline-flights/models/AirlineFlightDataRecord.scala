package pipelines.examples.modelserving.airlineflights.models

import pipelines.examples.modelserving.airlineflights.data.AirlineFlightRecord
import com.lightbend.modelserving.model.DataToServe

case class AirlineFlightDataRecord(record: AirlineFlightRecord) extends DataToServe {
  def getType: String = record.dataType
  def getRecord: AnyVal = record.asInstanceOf[AnyVal]
}
