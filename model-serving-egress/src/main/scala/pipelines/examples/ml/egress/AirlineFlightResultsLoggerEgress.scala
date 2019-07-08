package pipelines.examples.ml.egress

import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

object AirlineFlightResultsLoggerEgress extends PrintlnLoggerEgress[AirlineFlightResult] {
  val prefix: String = "Airline Flight Delay Prediction: "
}
