package pipelines.egress

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import akka.testkit._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.testkit._
import pipelines.test.{ OutputInterceptor, TestData }
import pipelines.logging.StdoutStderrLogger
import com.typesafe.config.ConfigFactory

class ConsoleEgressLogicTest extends FunSpec with BeforeAndAfterAll with OutputInterceptor {

  private implicit val system = ActorSystem("ConsoleEgressLogic")
  private implicit val mat = ActorMaterializer()

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  final class TestEgress extends AkkaStreamlet {
    val inlet = AvroInlet[TestData]("in")
    final override val shape = StreamletShape.withInlets(inlet)

    override def createLogic = ConsoleEgressLogic[TestData](
      in = inlet,
      prefix = "TestPrefix")
  }

  describe("LogEgress") {
    // I would prefer to test the output, but the output isn't captured, even with
    // the logger logic above that replaces the Akka logger with the "stdout/stderr"
    // logger. I suspect it's because the code is actually run by Akka on a different thread.
    it("Writes output to stdout - DOESN'T CURRENTLY TEST ANYTHING!!") {
      val data = Vector(TestData(1, "one"), TestData(2, "two"), TestData(3, "three"))
      // val expectedOut = data.map(_.toString)
      // expectOutput(expectedOut) {
      ignoreOutput {
        val testEgress = new TestEgress()
        val testkit = AkkaStreamletTestKit(system, mat, ConfigFactory.load())
        val source = Source(data)
        val in = testkit.inletFromSource(testEgress.inlet, source)
        testkit.run(testEgress, in, Nil, () ⇒ {})
      }
      Thread.sleep(1000) // give it time to write stdout before shutting down!
    }
  }
}
