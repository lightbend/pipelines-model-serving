package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroOutlet

import pipelines.examples.data._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Random

/**
 * Ingress of data for recommendations. In this case, every second we
 * load and send downstream one record that is randomly generated.
 */
class RecommenderDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[RecommenderRecord]("out", _.user.toString)
  final override val shape = StreamletShape.withOutlets(out)

  override final def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph =
      source.to(atLeastOnceSink(out))
  }

  protected lazy val dataFrequencySeconds =
    context.config.getInt("recommenders.data-frequency-seconds")

  def source: Source[RecommenderRecord, NotUsed] = {
    Source.repeat(NotUsed)
      .map(_ ⇒ RecommenderDataIngress.makeRecommenderRecord())
      .throttle(1, dataFrequencySeconds.seconds)
  }
}

object RecommenderDataIngress {
  val generator = Random

  def makeRecommenderRecord(): RecommenderRecord = {
    val user = generator.nextInt(1000).toLong
    val nprods = generator.nextInt(30)
    val products = new ListBuffer[Long]()
    0 to nprods foreach { _ ⇒ products += generator.nextInt(300).toLong }
    new RecommenderRecord(user, products, "recommender")
  }

  def main(args: Array[String]): Unit = {
    val ingress = new RecommenderDataIngress()
    ingress.source.runWith(Sink.foreach(println))
  }
}
