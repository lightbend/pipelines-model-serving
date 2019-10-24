package pipelines.examples.modelserving.airlineflights.models

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.modelserving.model.IngressTestBase
import pipelines.examples.modelserving.airlineflights.AirlineFlightRecordIngressUtil
import pipelines.examples.modelserving.airlineflights.data.AirlineFlightRecord

import scala.concurrent.duration.FiniteDuration

class AirlineFlightRecordIngressTest extends IngressTestBase[AirlineFlightRecord](
  count = 10,
  frequencyMillis = AirlineFlightRecordIngressUtil.dataFrequencyMilliseconds) {

  override protected def makeSource(frequency: FiniteDuration): Source[AirlineFlightRecord, NotUsed] =
    AirlineFlightRecordIngressUtil.makeSource(
      AirlineFlightRecordIngressUtil.rootConfigKey, frequency)

  "Loading a ingress data" should "succeed" in {
    readData()
  }
}
