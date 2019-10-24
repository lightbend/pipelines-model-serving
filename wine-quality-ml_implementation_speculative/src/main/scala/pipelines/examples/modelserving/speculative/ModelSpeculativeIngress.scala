package pipelines.examples.modelserving.speculative

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.modelserving.speculative.SpeculativeStreamMerger
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape

import scala.concurrent.duration._

/**
 * Ingress of model speculative definition updates. In this case, every five minutes we load and
 * send downstream a new speculative configuration
 */
final case object ModelSpeculativeIngress extends AkkaStreamlet {

  val out = AvroOutlet[SpeculativeStreamMerger]("out", _.getSchema.getName)

  final override val shape = StreamletShape.withOutlets(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph =
      ModelSpeculativeIngressUtil.makeSource().to(plainSink(out))
  }
}

object ModelSpeculativeIngressUtil {

  val speculative1 = SpeculativeStreamMerger(1000L, 2)
  val speculative2 = SpeculativeStreamMerger(1200L, 2)
  val speculative3 = SpeculativeStreamMerger(1500L, 2)
  val speculative4 = SpeculativeStreamMerger(900L, 2)
  val speculativeDefs = List(speculative1, speculative2, speculative3, speculative4)
  var iterator = speculativeDefs.iterator

  /** Helper method extracted from RecommenderModelIngress for easier unit testing. */
  def makeSource(
      frequency: FiniteDuration = 300.seconds): Source[SpeculativeStreamMerger, NotUsed] =
    Source.repeat(generateSplit())
      .throttle(1, frequency)

  def generateSplit(): SpeculativeStreamMerger = {
    if (!iterator.hasNext) iterator = speculativeDefs.iterator
    iterator.next()
  }
}
