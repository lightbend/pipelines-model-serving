package pipelines.examples.modelserving.winequality

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.modelserving.model.IngressTestBase
import pipelines.examples.modelserving.winequality.data.WineRecord

import scala.concurrent.duration._

import scala.concurrent.duration.FiniteDuration

class WineRecordIngressTest extends IngressTestBase[WineRecord](
  count = 10,
  frequencyMillis = 1.second) {

  override protected def makeSource(frequency: FiniteDuration): Source[WineRecord, NotUsed] =
    WineRecordIngressUtil.makeSource(
      WineRecordIngressUtil.rootConfigKey, frequency)

  "Loading a ingress data" should "succeed" in {
    readData()
  }
}
