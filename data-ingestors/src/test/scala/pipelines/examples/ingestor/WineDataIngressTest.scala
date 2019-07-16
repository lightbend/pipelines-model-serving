package pipelines.examples.ingestor

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import akka.testkit._
import akka.actor._
import akka.stream._
import scala.io.Source
import pipelines.akkastream.testkit._
import pipelines.examples.data.WineRecord
import pipelines.test.OutputInterceptor
import com.typesafe.config.ConfigFactory

class WineDataIngressTest extends FunSpec with BeforeAndAfterAll with OutputInterceptor {

  val initializingMsgFmt = "RecordsReader: Initializing from resource %s"
  // val filePrefix = "data-ingestors/src/test/resources/"
  val filePrefix = "data-ingestors/target/scala-2.12/test-classes/"
  val testGoodRecordsResources = Array("wine/data/10_winequality_red.csv")
  val testBadRecordsResources = Array("wine/data/error_winequality_red.csv")
  val emptyOutput: Array[String] = Array()
  private implicit val system = ActorSystem("WineDataIngress")
  private implicit val mat = ActorMaterializer()

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def toKeyedWineRecord(s: String): (String, WineRecord) = {
    val rec = WineDataIngressUtil.parse(s).right.get
    (rec.dataType, rec)
  }

  def expected(sources: Seq[String]): Vector[(String, WineRecord)] = {

    val useFiles = true // hack
    val sources2 = if (useFiles) sources.map(s ⇒ filePrefix + s) else sources
    val exp = sources2.foldLeft(Vector.empty[String]) { (vect, source) ⇒
      val is =
        if (useFiles) Source.fromFile(source)
        else Source.fromResource(source)
      val lines = is.getLines.toVector
      vect ++ lines
      is.close()
      vect
    }.map(toKeyedWineRecord)
    assert(exp.size > 0, bugMsg(sources2))
    exp
  }

  def bugMsg(sources: Seq[String]): String = {
    val sourcesStr = sources.mkString("[", ", ", "]")
    s"TEST BUG: (PWD: ${sys.env("PWD")}) Failed to load expected data from $sourcesStr"
  }

  describe("WineDataIngress") {
    it("Loads one or more CSV file resources from the classpath") {
      // NOTE: If this test fails, try running it again. For some reason, it sometimes
      // appears to not load the data or have too much of it! Then the very last check
      // for "Completed" fails. Obviously, I'd love for someone to figure out why this
      // happens...
      ignoreOutput {
        val config = ConfigFactory.load()
        val testkit = AkkaStreamletTestKit(system, mat, config)
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
