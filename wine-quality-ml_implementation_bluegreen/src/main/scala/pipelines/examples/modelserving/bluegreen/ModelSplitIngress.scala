package pipelines.examples.modelserving.bluegreen

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape

import scala.concurrent.duration._
import com.lightbend.modelserving.splitter.{ OutputPercentage, StreamSplitter }

import scala.collection.mutable.ListBuffer

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

  val percentage1 = Array(10, 90)
  val percentage2 = Array(40, 60)
  val percentage3 = Array(60, 40)
  val percentage4 = Array(90, 10)
  val percentageDefs = List(percentage1, percentage2, percentage3, percentage4)
  var iterator = percentageDefs.iterator

  /** Helper method extracted from RecommenderModelIngress for easier unit testing. */
  def makeSource(
      frequency: FiniteDuration = 120.seconds): Source[StreamSplitter, NotUsed] =
    Source.repeat(generateSplit())
      .throttle(1, frequency)

  def generateSplit(): StreamSplitter = {
    if (!iterator.hasNext) iterator = percentageDefs.iterator
    val percents = iterator.next()
    val inputs = new ListBuffer[OutputPercentage]()
    0 to 1 foreach (i ⇒ {
      val output = i
      val percentage = percents(i)
      inputs += new OutputPercentage(output, percentage)
    })
    new StreamSplitter(inputs)
  }
}
