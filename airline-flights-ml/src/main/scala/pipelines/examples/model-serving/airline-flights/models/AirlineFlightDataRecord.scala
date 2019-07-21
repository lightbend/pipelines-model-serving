package pipelines.examples.modelserving.airlineflights.models

import pipelines.examples.modelserving.airlineflights.data.AirlineFlightRecord
import com.lightbend.modelserving.model.DataToServe

case class AirlineFlightDataRecord(record: AirlineFlightRecord) extends DataToServe[AirlineFlightRecord] {
  def getType: String = record.dataType
}
