package pipelines.examples.ingestor

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelines.examples.data._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.concurrent.duration._
import pipelines.examples.util.ConfigUtil
import pipelines.examples.util.ConfigUtil.implicits._

/**
 * Ingress of data for recommendations. In this case, every second we
 * load and send downstream one record that is randomly generated.
 */
class RecommenderDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[RecommenderRecord]("out", _.user.toString)
  final override val shape = StreamletShape.withOutlets(out)

  protected lazy val configUtil = ConfigUtil.default
  protected lazy val dataFrequencySeconds =
    configUtil.getOrElse[Int]("recommenders.data-frequency-seconds")(1).seconds

  val source: Source[RecommenderRecord, NotUsed] =
    Source.repeat(NotUsed)
      .map(_ ⇒ RecommenderDataIngress.makeRecommenderRecord())
      .throttle(1, dataFrequencySeconds)

  override final def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph = source.to(atMostOnceSink(out))
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
    implicit val system = ActorSystem("RecommenderDataIngress-Main")
    implicit val mat = ActorMaterializer()
    val ingress = new RecommenderDataIngress()
    println(s"frequency (seconds): ${ingress.dataFrequencySeconds}")
    ingress.source.runWith(Sink.foreach(println))
  }
}
