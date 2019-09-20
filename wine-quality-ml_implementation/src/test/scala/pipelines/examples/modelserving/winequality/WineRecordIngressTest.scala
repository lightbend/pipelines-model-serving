package pipelines.examples.modelserving.winequality

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import akka.testkit._
import akka.actor._
import akka.stream._
import scala.io.Source
import pipelines.akkastream.testkit._
import com.typesafe.config.ConfigFactory
import pipelinesx.test.OutputInterceptor
import pipelines.examples.modelserving.winequality.data.WineRecord

class WineRecordIngressTest extends FunSpec with BeforeAndAfterAll with OutputInterceptor {

  val initializingMsgFmt = "RecordsReader: Initializing from resource %s"
  // val filePrefix = "wine-quality-ml/target/scala-2.12/test-classes/"
  val filePrefix = "target/scala-2.12/test-classes/"
  val testGoodRecordsResources = Array("wine/data/10_winequality_red.csv")
  val testBadRecordsResources = Array("wine/data/error_winequality_red.csv")
  val emptyOutput: Array[String] = Array()
  private implicit val system = ActorSystem("WineRecordIngress")
  private implicit val mat = ActorMaterializer()

  override def afterAll: Unit = {
    resetOutputs()
    TestKit.shutdownActorSystem(system)
  }

  def toKeyedWineRecord(s: String): (String, WineRecord) = {
    val rec = WineRecordIngressUtil.parse(s).right.get
    (rec.lot_id, rec)
  }

  def expected(sources: Seq[String]): Vector[(String, WineRecord)] = {

    val useFiles = true // hack
    val sources2 = if (useFiles) sources.map(s ⇒ filePrefix + s) else sources
    val exp = sources2.foldLeft(Vector.empty[String]) { (vect, source) ⇒
      val is =
        if (useFiles) Source.fromFile(source)
        else Source.fromResource(source)
      val lines = is.getLines.toVector
      val vect2 = vect ++ lines
      is.close()
      vect2
    }.map(toKeyedWineRecord)
    assert(exp.size > 0, bugMsg(sources2))
    exp
  }

  def bugMsg(sources: Seq[String]): String = {
    val s = new StringBuilder
    (filePrefix +: sources).foreach { str ⇒
      s.append(str).append(": ")
      val f = new java.io.File(str)
      s.append("exists? ").append(f.exists)
      s.append(" is directory? ").append(f.isDirectory)
      if (f.isDirectory) s.append(". list: ").append(f.listFiles.mkString("[", ", ", "]"))
      s.append("\n")
    }
    val sourcesStr = sources.mkString("[", ", ", "]")
    s"TEST BUG: (PWD: ${sys.env("PWD")}) Failed to load expected data from $sourcesStr\n$s"
  }

  describe("WineRecordIngress") {
    it("Loads one or more CSV file resources from the CLASSPATH") {
      // NOTE: If this test fails, try running it again. For some reason, it sometimes
      // appears to not load the data or have too much of it! Then the very last check
      // for "Completed" fails. Obviously, I'd love for someone to figure out why this
      // happens...
      // Also, "ignoreOutput" doesn't appear to capture all output!!
      ignoreOutput {
        val config = ConfigFactory.load()
        val testkit = AkkaStreamletTestKit(system, mat, config)
        val ingress = WineRecordIngress // Relies on the .../test/resources/application.conf to point to the correct files
        val out = testkit.outletAsTap(ingress.out)

        val exp = expected(testGoodRecordsResources)
        testkit.run(ingress, Nil, out, () ⇒ {
          exp.foreach { e ⇒ out.probe.expectMsg(e) }
        })

        out.probe.expectMsg(Completed)
        ()
      }
    }
  }
}
