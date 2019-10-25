package pipelines.examples.modelserving.recommender

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelIngressTestBase }
import scala.concurrent.duration._

import scala.concurrent.duration.FiniteDuration

class RecommenderModelIngressTest extends ModelIngressTestBase(
  count = 10,
  frequencyMillis = 1.second) {

  override protected def makeSource(frequency: FiniteDuration): Source[ModelDescriptor, NotUsed] =
    RecommenderModelIngressUtil.makeSource(
      RecommenderModelIngressUtil.recommenderServerLocations, frequency)

  "Loading a ingress data" should "succeed" in {
    readData()
  }
}
