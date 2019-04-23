package pipelines.examples.ingestor

import akka.http.scaladsl.model.headers.Date
import pipelines.akkastream.scaladsl.{ FlowLogic, FlowProcessor }
import pipelines.examples.data._
import pipelines.examples.data.Codecs._

object FormatMLModel extends FlowProcessor[ModelDescriptor, MlAction] {
  override def createLogic = new FlowLogic() {
    def flow = contextPropagatedFlow().map(model ⇒ {
      MlAction("UPDATE_MODEL", model, WineRecord("", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    })
  }
}
