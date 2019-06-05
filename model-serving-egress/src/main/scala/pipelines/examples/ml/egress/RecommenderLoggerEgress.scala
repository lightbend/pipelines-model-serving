package pipelines.examples.ml.egress

import pipelines.akkastream.scaladsl.{ FlowEgress, FlowEgressLogic }
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

object RecommenderLoggerEgress extends FlowEgress[RecommendationResult] {

  override def createLogic = new FlowEgressLogic() {

    def flow = contextPropagatedFlow()
      .map { result ⇒
        {
          if (result.result.size > 0)
            println(s"Result: $result")
          result
        }
      }
  }
}
