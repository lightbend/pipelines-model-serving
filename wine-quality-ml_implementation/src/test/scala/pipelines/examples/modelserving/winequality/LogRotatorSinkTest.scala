package pipelines.examples.modelserving.winequality

import java.nio.file.{ FileSystems, Path }
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.ByteString
import com.lightbend.modelserving.model.util.MainBase
import org.scalatest.FlatSpec

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import pipelines.examples.modelserving.winequality.data.WineRecord

class LogRotatorSinkTest extends FlatSpec {

  // Make sure that directory here exists, otherwise LogRotatorSink will quietly not write anything
  private val destinationDir = FileSystems.getDefault.getPath("incoming")
  private val formatter = DateTimeFormatter.ofPattern("'stream-'yyyy-MM-dd_HH'.messages'")

  implicit val system = ActorSystem("ModelServing")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val timeBasedTriggerCreator: () ⇒ ByteString ⇒ Option[Path] = () ⇒ {
    var currentFilename: Option[String] = None
    (_: ByteString) ⇒ {
      val newName = LocalDateTime.now().format(formatter)
      // Hourly
      if (currentFilename.contains(newName)) {
        None
      } else {
        currentFilename = Some(newName)
        val file = destinationDir.resolve(newName)
        println(s"Writing to a file ${file.toString}")
        Some(file)
      }
    }
  }

  val timeBasedSink: Sink[ByteString, Future[Done]] =
    LogRotatorSink(timeBasedTriggerCreator)

  ensureFileExists()

  "Incomming messages" should "writtent to file" in {
    WineRecordIngressReader.makeSource(1.milliseconds).map(m ⇒ ByteString(m.toString + "\n"))
      .runWith(timeBasedSink)
    Thread.sleep(6000)
  }

  // Ensure that output file exists
  private def ensureFileExists(): Unit = {
    val dir = destinationDir.toFile
    // make sure all the parent directories exist.
    if (!dir.exists()) dir.mkdir()
  }
}

object WineRecordIngressReader extends MainBase[WineRecord](
  defaultCount = 10,
  defaultFrequencyMillis = WineRecordIngressUtil.dataFrequencyMilliseconds) {

  override def makeSource(frequency: FiniteDuration): Source[WineRecord, NotUsed] =
    WineRecordIngressUtil.makeSource(
      WineRecordIngressUtil.rootConfigKey, frequency)
}
