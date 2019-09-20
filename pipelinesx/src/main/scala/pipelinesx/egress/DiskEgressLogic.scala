package pipelinesx.egress

import java.nio.file.{ FileSystems, Path }
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.Done
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import pipelines.akkastream._
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.streamlets._

import scala.concurrent.Future

/**
 * Abstraction for writing to output to Disk.
 * @param in CodecInlet for records of type IN
 * @param outdir output directory
 * @param separator String, which defaults to [["\n"]].
 */
final case class DiskEgressLogic[IN](
    in:        CodecInlet[IN],
    outdir:    String,
    separator: String         = "\n")(
    implicit
    val context: StreamletContext)
  extends RunnableGraphStreamletLogic {

  private val destinationDir = FileSystems.getDefault.getPath(s"$outdir/${context.streamletRef}")
  private val formatter = DateTimeFormatter.ofPattern("'stream-'yyyy-MM-dd_HH'.messages'")
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

  val timeBasedSink: Sink[ByteString, Future[Done]] = LogRotatorSink(timeBasedTriggerCreator)

  val dir = destinationDir.toFile
  // make sure all the parent directories exist.
  if (!dir.exists()) dir.mkdir()

  override def runnableGraph(): RunnableGraph[_] = atMostOnceSource(in)
    .map(m ⇒ ByteString(m.toString + separator))
    .to(timeBasedSink)
}
