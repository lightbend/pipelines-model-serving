package pipelines.examples.modelserving.recommender

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.concurrent.duration._
import pipelinesx.config.ConfigUtil
import pipelinesx.config.ConfigUtil.implicits._
import com.lightbend.modelserving.model.util.MainBase
import pipelines.examples.modelserving.recommender.data.RecommenderRecord

/**
 * Ingress of data for recommendations. In this case, every second we
 * load and send downstream one record that is randomly generated.
 */
final case object RecommenderRecordIngress extends AkkaStreamlet {

  val out = AvroOutlet[RecommenderRecord]("out", _.user.toString)

  final override val shape = StreamletShape.withOutlets(out)

  override final def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      RecommenderRecordIngressUtil.makeSource().to(plainSink(out))
  }
}

object RecommenderRecordIngressUtil {

  lazy val dataFrequencyMilliseconds: FiniteDuration =
    ConfigUtil.default.getOrElse[Int]("recommender.data-frequency-milliseconds")(1).milliseconds

  def makeSource(
      frequency: FiniteDuration = dataFrequencyMilliseconds): Source[RecommenderRecord, NotUsed] = {
    Source.repeat(RecommenderRecordMaker)
      .map(maker ⇒ maker.make())
      .throttle(1, frequency)
  }

  protected lazy val generator = Random

  object RecommenderRecordMaker {
    def make(): RecommenderRecord = {
      val user = generator.nextInt(1000).toLong
      val nprods = generator.nextInt(30)
      val products = new ListBuffer[Long]()
      0 to nprods foreach { _ ⇒ products += generator.nextInt(300).toLong }
      new RecommenderRecord(user, products)
    }
  }
}

/**
 * Test program for [[RecommenderRecordIngress]] and [[RecommenderRecordIngressUtil]];
 * reads records and prints them. For testing purposes only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.recommender._
 * RecommenderRecordIngressMain.main(Array("-n","10","-f","1000"))
 * ```
 */
object RecommenderRecordIngressMain extends MainBase[RecommenderRecord](
  defaultCount = 10,
  defaultFrequencyMillis = RecommenderRecordIngressUtil.dataFrequencyMilliseconds) {

  override protected def makeSource(frequency: FiniteDuration): Source[RecommenderRecord, NotUsed] =
    RecommenderRecordIngressUtil.makeSource(frequency)
}
