package pipelines.examples.ml.egress

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import akka.testkit._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import pipelines.akkastream.testkit._
import pipelines.examples.util.test.{ OutputInterceptor, TestData }
import pipelines.examples.util.test.TestDataCodecs._
import com.typesafe.config.ConfigFactory

class PrintlnLoggerEgressTest extends FunSpec with BeforeAndAfterAll with OutputInterceptor {

  private implicit val system = ActorSystem("PrintlnLoggerEgress")
  private implicit val mat = ActorMaterializer()

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  class TestEgress extends PrintlnLoggerEgress[TestData] {
    val prefix: String = "TestPrefix"
  }

  dumpOutputStreams = true

  describe("PrintlnLoggerEgress") {
    it("Writes output to stdout - CURRENTLY DOESN'T WORK!!") {
      val data = Vector(TestData(1, "one"), TestData(2, "two"), TestData(3, "three"))
      // val expectedOut = data.map(_.toString)
      // expectOutput(expectedOut) {
      ignoreOutput {
        val testkit = AkkaStreamletTestKit(system, mat, ConfigFactory.load())
        val egress = new TestEgress
        // val in = testkit.inletAsTap[TestData](egress.shape.inlet)
        // data.foreach(d ⇒ in.queue.offer(d))
        val source = Source(data)
        val in = testkit.inletFromSource(egress.shape.inlet, source)
        testkit.run(egress, in, Nil, () ⇒ {})
        Thread.sleep(1000) // give it time to write stdout before shutting down!
      }
    }
  }
}
