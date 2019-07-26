package pipelines.examples.modelserving.airlineflights

import pipelines.examples.modelserving.airlineflights.data._
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelinesx.egress.ConsoleEgressLogic

final case object AirlineFlightResultConsoleEgress extends AkkaStreamlet {
  val in = AvroInlet[AirlineFlightResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = ConsoleEgressLogic[AirlineFlightResult](
    in = in,
    prefix = "Airline Flight Delay Prediction: ")
}
