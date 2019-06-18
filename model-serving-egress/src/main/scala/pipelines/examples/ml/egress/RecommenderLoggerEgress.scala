package pipelines.examples.ml.egress

import pipelines.streamlets.avro.AvroInlet
import pipelines.egress.LogEgress
import pipelines.examples.data._
import akka.event.Logging.{ LogLevel, InfoLevel }

final case class RecommenderLoggerEgress(logLevel: LogLevel = InfoLevel)
  extends LogEgress {
  val prefix: String = "Recommender: "
  type IN = RecommendationResult
  val in = AvroInlet[RecommendationResult]("in")
}
