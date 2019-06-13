package pipelines.examples.ml.egress

import pipelines.examples.data._
import akka.event.Logging.{ LogLevel, InfoLevel }

final case class RecommenderLoggerEgress(logLevel: LogLevel = InfoLevel)
  extends LogEgress[RecommendationResult](logLevel) {
  val prefix: String = "Recommender: "
}
