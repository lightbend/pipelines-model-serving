package pipelines.examples.ml.egress

import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

object RecommenderLoggerEgress extends PrintlnLoggerEgress[RecommendationResult] {
  val prefix: String = "Recommender: "
}
