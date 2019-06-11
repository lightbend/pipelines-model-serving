package pipelines.examples.ingestor

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import akka.testkit._
import akka.actor._
import akka.stream._
import pipelines.akkastream.testkit._
import pipelines.examples.data.WineRecord
import pipelines.examples.data.DataCodecs._
import com.typesafe.config.ConfigFactory

class WineDataIngressTest extends FunSpec with BeforeAndAfterAll with OutputInterceptor {

  val initializingMsgFmt = "WineDataIngress: Initializing from resource %s"
  val testGoodRecordsResources = Array("10_winequality_red.csv")
  val testBadRecordsResources = Array("error_winequality_red.csv")

  private implicit val system = ActorSystem("WineDataIngress")
  private implicit val mat = ActorMaterializer()

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def toKeyedWineRecord(s: String): (String, WineRecord) = {
    val rec = WineRecordsReader.parse(s, ";").get
    (rec.dataType, rec)
  }

  def expected(sources: Seq[String]): Vector[(String, WineRecord)] =
    sources.foldLeft(Vector.empty[String]) { (vect, source) ⇒
      vect ++ scala.io.Source.fromResource(source).getLines.toVector
    }.map(toKeyedWineRecord)

  describe("WineDataIngress") {
    it("Loads one or more CSV file resources from the classpath") {
      expectOutput(Seq("WineRecordsReader: Initializing from resource 10_winequality_red.csv")) {
        val testkit = AkkaStreamletTestKit(system, mat, ConfigFactory.load())
        val ingress = new WineDataIngress
        val out = testkit.outletAsTap(ingress.shape.outlet)

        ingress.recordsResources = testGoodRecordsResources
        val exp = expected(testGoodRecordsResources)
        testkit.run(ingress, Nil, out, () ⇒ {
          exp.foreach { e ⇒ out.probe.expectMsg(e) }
        })

        out.probe.expectMsg(Completed)
      }
    }
  }

  // This doesn't work: The full application.conf is apparently being loaded anyway,
  // so it finds the "good" file.
  // it("Raises an exception if the resource doesn't exist") {
  //   import scala.collection.JavaConverters._
  //   intercept[IllegalArgumentException] {
  //     val config1 = ConfigFactory.parseMap(
  //       Map(
  //         "wine-quality.data-sources" -> (Seq("fail").asJava),
  //         "wine-quality.data-frequency-seconds" -> 1
  //       ).asJava)
  //     val config = config1 //.withFallback(ConfigFactory.load())
  //     val testkit = AkkaStreamletTestKit(system, mat, config)
  //     val ingress = new WineDataIngress
  //     val out = testkit.outletAsTap(ingress.shape.outlet)
  //     testkit.run(ingress, Nil, out, () ⇒ {})
  //   }
  // }

}