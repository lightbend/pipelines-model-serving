package pipelines.examples.ingestor

import pipelines.akkastream.scaladsl._
import pipelines.examples.data.Codecs._
import pipelines.examples.data._

class ModelUpdateLogger extends FlowEgress[ModelUpdateConfirm] {
  override def createLogic = new FlowEgressLogic[ModelUpdateConfirm]() {
    def flow = {
      contextPropagatedFlow()
        .map { m ⇒
          val name = m.name
          system.log.warning(s"Model updated! $name")
          m
        }
    }
  }
}

