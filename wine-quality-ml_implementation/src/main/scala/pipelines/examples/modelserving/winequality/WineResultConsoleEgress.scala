package pipelines.examples.modelserving.winequality

import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelinesx.egress.ConsoleEgressLogic
import pipelines.examples.modelserving.winequality.data.WineResult

final case object WineResultConsoleEgress extends AkkaStreamlet {
  val in = AvroInlet[WineResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = ConsoleEgressLogic[WineResult](
    in = in,
    prefix = "Wine Quality: ")
}

