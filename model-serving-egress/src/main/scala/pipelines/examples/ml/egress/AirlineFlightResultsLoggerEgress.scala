package pipelines.examples.ml.egress

import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelines.egress.LogEgressLogic
import pipelines.examples.data._

final case object AirlineFlightResultsLoggerEgress extends AkkaStreamlet {
  val in = AvroInlet[AirlineFlightResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = LogEgressLogic.makeFromConfig[AirlineFlightResult](
    in = in,
    logLevelConfigKey = "airline-flights.log-level",
    prefix = "Airline Flight Delay Prediction: ")
}
