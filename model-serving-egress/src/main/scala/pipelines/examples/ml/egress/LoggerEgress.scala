package pipelines.examples.ml.egress

import pipelines.akkastream.scaladsl.{ FlowEgress, FlowEgressLogic }
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

object LoggerEgress extends FlowEgress[RecommendationResult] {

  override def createLogic = new FlowEgressLogic() {

    def flow = contextPropagatedFlow()
      .map { result ⇒
        {
          result.result.size match {
            case 0 ⇒
            case _ ⇒
              println(s"Result: $result")
          }
          result
        }
      }
  }
}
