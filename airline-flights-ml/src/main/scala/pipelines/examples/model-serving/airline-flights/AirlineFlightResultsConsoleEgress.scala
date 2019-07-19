package pipelines.examples.modelserving.airlineflights

import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelines.egress.ConsoleEgressLogic
import pipelines.examples.data._

final case object AirlineFlightResultsConsoleEgress extends AkkaStreamlet {
  val in = AvroInlet[AirlineFlightResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = ConsoleEgressLogic[AirlineFlightResult](
    in = in,
    prefix = "Airline Flight Delay Prediction: ")
}
