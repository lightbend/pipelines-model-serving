package pipelines.examples.ingestor

import pipelines.akkastream.scaladsl._
import pipelines.examples.data.InvalidRecord
import pipelines.examples.data.Codecs._

class InvalidDataLogger extends FlowEgress[InvalidRecord] {
  override def createLogic = new FlowEgressLogic[InvalidRecord]() {
    def flow = {
      contextPropagatedFlow()
        .map { invalidRecord ⇒
          system.log.warning(s"Invalid metric detected! $invalidRecord")
          invalidRecord
        }
    }
  }
}

