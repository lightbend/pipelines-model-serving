package pipelines.examples.ml.egress

import pipelines.akkastream.AkkaStreamlet
import pipelines.egress.ConsoleEgressLogic
import pipelines.examples.data._
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet

final case object WineModelUpdateConsoleEgress extends AkkaStreamlet {
  val in = AvroInlet[ModelDescriptor]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = ConsoleEgressLogic[ModelDescriptor](
    in = in,
    prefix = "Wine Quality: ")
}

