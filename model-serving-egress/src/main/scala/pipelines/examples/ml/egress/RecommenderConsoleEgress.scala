package pipelines.examples.ml.egress

import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelines.egress.ConsoleEgressLogic
import pipelines.examples.data._

final case object RecommenderConsoleEgress extends AkkaStreamlet {
  val in = AvroInlet[RecommendationResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = ConsoleEgressLogic.make[RecommendationResult](
    in = in,
    prefix = "Recommender: ")
}
