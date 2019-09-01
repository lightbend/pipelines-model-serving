package pipelines.examples.modelserving.airlineflights

import akka.stream.scaladsl.Sink
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.examples.modelserving.airlineflights.data.AirlineFlightResult

final case object AirlineFlightResultConsoleEgress extends AkkaStreamlet {
  val inlet = AvroInlet[AirlineFlightResult]("in")
  final override val shape = StreamletShape.withInlets(inlet)

  override def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      atMostOnceSource(inlet).to(Sink.foreach(line ⇒ println("Airline Flight Delay Prediction: " + line)))
  }
}
