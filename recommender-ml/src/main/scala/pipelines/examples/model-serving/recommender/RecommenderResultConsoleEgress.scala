package pipelines.examples.modelserving.recommender

import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelines.examples.modelserving.recommender.data._
import pipelinesx.egress.ConsoleEgressLogic

final case object RecommenderResultConsoleEgress extends AkkaStreamlet {
  val in = AvroInlet[RecommenderResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = ConsoleEgressLogic[RecommenderResult](
    in = in,
    prefix = "Recommender: ")
}
