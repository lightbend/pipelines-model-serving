package pipelines.examples.modelserving.winequality

import java.nio.file.{ FileSystems, Path }
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, OverflowStrategy }
import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.ByteString
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

  val queue = Source.queue[String](3, OverflowStrategy.fail)
    .to(Sink.foreach(v ⇒ println(s"getting $v from the queue")))
    .run

  val timeBasedTriggerCreator: () ⇒ ByteString ⇒ Option[Path] = () ⇒ {
    var currentFilename: Option[String] = None
    (_: ByteString) ⇒ {
      val newName = LocalDateTime.now().format(formatter)
      // Hourly
      currentFilename match {
        case Some(file) ⇒ queue.offer(file)
        case _          ⇒
      }
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
    makeSource(1.seconds).map(m ⇒ ByteString(m.toString + "\n"))
      .runWith(timeBasedSink)
    Thread.sleep(6000)
  }

  // Ensure that output file exists
  private def ensureFileExists(): Unit = {
    val dir = destinationDir.toFile
    // make sure all the parent directories exist.
    if (!dir.exists()) dir.mkdir()
    ()
  }

  def makeSource(frequency: FiniteDuration): Source[WineRecord, NotUsed] =
    WineRecordIngressUtil.makeSource(
      WineRecordIngressUtil.rootConfigKey, frequency)

}
