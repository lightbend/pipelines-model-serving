package pipelines.examples.ml.egress

import pipelines.akkastream.scaladsl.{ FlowEgress, FlowEgressLogic }
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

object WineResultLoggerEgress extends FlowEgress[WineResult] {

  override def createLogic = new FlowEgressLogic() {

    def flow = contextPropagatedFlow()
      .map { result ⇒
        {
          println(s"Result: $result")
          result
        }
      }
  }
}
