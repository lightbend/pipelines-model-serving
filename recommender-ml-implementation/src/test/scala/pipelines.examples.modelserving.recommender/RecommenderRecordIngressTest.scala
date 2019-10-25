package pipelines.examples.modelserving.recommender

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.modelserving.model.IngressTestBase
import pipelines.examples.modelserving.recommender.data.RecommenderRecord
import scala.concurrent.duration._

import scala.concurrent.duration.FiniteDuration

class RecommenderRecordIngressTest extends IngressTestBase[RecommenderRecord](
  count = 10,
  frequencyMillis = 1.second) {

  override protected def makeSource(frequency: FiniteDuration): Source[RecommenderRecord, NotUsed] =
    RecommenderRecordIngressUtil.makeSource(frequency)

  "Loading a ingress data" should "succeed" in {
    readData()
  }
}
