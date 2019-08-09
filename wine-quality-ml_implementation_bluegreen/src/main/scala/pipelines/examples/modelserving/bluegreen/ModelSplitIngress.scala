package pipelines.examples.modelserving.bluegreen

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape

import scala.concurrent.duration._
import com.lightbend.modelserving.splitter.StreamSplitter

/**
 * Ingress of model splitting updates. In this case, every two minutes we load and
 * send downstream a new splitting configuration
 */
final case object ModelSplitIngress extends AkkaStreamlet {

  val out = AvroOutlet[StreamSplitter]("out", _.getSchema.getName)

  final override val shape = StreamletShape.withOutlets(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph =
      ModelSplitIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

object ModelSplitIngressUtil {

  /** Helper method extracted from RecommenderModelIngress for easier unit testing. */
  def makeSource(
      frequency: FiniteDuration = 120.seconds): Source[StreamSplitter, NotUsed] = ???
}
