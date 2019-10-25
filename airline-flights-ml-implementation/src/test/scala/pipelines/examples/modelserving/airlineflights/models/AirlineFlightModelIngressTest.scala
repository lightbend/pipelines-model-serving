package pipelines.examples.modelserving.airlineflights.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelIngressTestBase }
import pipelines.examples.modelserving.airlineflights.AirlineFlightModelIngressUtil
import scala.concurrent.duration._

import scala.concurrent.duration.FiniteDuration

class AirlineFlightModelIngressTest extends ModelIngressTestBase(
  count = 10,
  frequencyMillis = 1.second) {

  override protected def makeSource(frequency: FiniteDuration): Source[ModelDescriptor, NotUsed] =
    AirlineFlightModelIngressUtil.makeSource(frequency)

  "Loading a ingress data" should "succeed" in {
    readData()
  }
}
