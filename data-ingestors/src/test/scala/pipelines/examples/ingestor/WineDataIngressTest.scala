package pipelines.examples.ingestor

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import akka.testkit._
import akka.actor._
import akka.stream._
import pipelines.akkastream.testkit._
import pipelines.examples.data.WineRecord
import pipelines.test.OutputInterceptor
import com.typesafe.config.ConfigFactory

class WineDataIngressTest extends FunSpec with BeforeAndAfterAll with OutputInterceptor {

  val initializingMsgFmt = "RecordsFilesReader: Initializing from resource %s"
  val testGoodRecordsResources = Array("wine/data/10_winequality_red.csv")
  val testBadRecordsResources = Array("wine/data/error_winequality_red.csv")

  private implicit val system = ActorSystem("WineDataIngress")
  private implicit val mat = ActorMaterializer()

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def toKeyedWineRecord(s: String): (String, WineRecord) = {
    val rec = WineDataIngressUtil.parse(s.split(";")).right.get
    (rec.dataType, rec)
  }

  def expected(sources: Seq[String]): Vector[(String, WineRecord)] =
    sources.foldLeft(Vector.empty[String]) { (vect, source) ⇒
      vect ++ scala.io.Source.fromResource(source).getLines.toVector
      vect
    }.map(toKeyedWineRecord)

  describe("WineDataIngress") {
    it("Loads one or more CSV file resources from the classpath") {
      expectOutput(testGoodRecordsResources.map(name ⇒ initializingMsgFmt.format(name))) {
        val testkit = AkkaStreamletTestKit(system, mat, ConfigFactory.load())
        val ingress = WineDataIngress // Relies on the .../test/resources/application.conf to point to the correct files
        val out = testkit.outletAsTap(ingress.out)

        val exp = expected(testGoodRecordsResources)
        testkit.run(ingress, Nil, out, () ⇒ {
          exp.foreach { e ⇒ out.probe.expectMsg(e) }
        })

        out.probe.expectMsg(Completed)
      }
    }
  }
}
