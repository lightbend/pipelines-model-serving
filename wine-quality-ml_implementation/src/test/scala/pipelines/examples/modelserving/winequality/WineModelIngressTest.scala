package pipelines.examples.modelserving.winequality

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelIngressTestBase }
import scala.concurrent.duration._

import scala.concurrent.duration.FiniteDuration

class WineModelIngressTest extends ModelIngressTestBase(
  count = 10,
  frequencyMillis = 1.second) {

  override protected def makeSource(frequency: FiniteDuration): Source[ModelDescriptor, NotUsed] =
    WineModelIngressUtil.makeSource(
      WineModelIngressUtil.wineModelsResources, frequency)

  "Loading a ingress data" should "succeed" in {
    readData()
  }
}
