package pipelines.egress

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import akka.testkit._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import pipelines.akkastream.testkit._
import pipelines.util.test.{ OutputInterceptor, TestData }
import com.typesafe.config.ConfigFactory

class LogEgressTest extends FunSpec with BeforeAndAfterAll with OutputInterceptor {

  private implicit val system = ActorSystem("LogEgress")
  private implicit val mat = ActorMaterializer()

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  object TestEgress extends LogEgress[TestData](akka.event.Logging.WarningLevel) {
    val prefix: String = "TestPrefix"
  }

  // dumpOutputStreams = true

  describe("LogEgress") {
    it("Writes output to stdout - DOESN'T CURRENTLY TEST ANYTHING!!") {
      val data = Vector(TestData(1, "one"), TestData(2, "two"), TestData(3, "three"))
      // I would prefer to test the output, but the output isn't captured!
      // I believe it's because the code is actually run by Akka on a different thread.
      // val expectedOut = data.map(_.toString)
      // expectOutput(expectedOut) {
      ignoreOutput {
        val testkit = AkkaStreamletTestKit(system, mat, ConfigFactory.load())
        val source = Source(data)
        val in = testkit.inletFromSource(TestEgress.in, source)
        testkit.run(TestEgress, in, Nil, () ⇒ {})
      }
      Thread.sleep(1000) // give it time to write stdout before shutting down!
    }
  }
}
