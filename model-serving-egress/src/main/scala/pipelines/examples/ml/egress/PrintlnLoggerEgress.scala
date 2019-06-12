package pipelines.examples.ml.egress

import pipelines.akkastream.scaladsl.{ FlowEgress, FlowEgressLogic }

trait PrintlnLoggerEgress[T] extends FlowEgress[T] {

  /** String to write at the beginning of each line/ */
  def prefix: String

  override def createLogic = new FlowEgressLogic() {

    def flow = {
      flowWithPipelinesContext()
        .map { result ⇒
          println(s"$prefix result: $result")
          result
        }
    }
  }
}
