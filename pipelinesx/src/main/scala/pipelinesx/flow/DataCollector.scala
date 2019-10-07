package pipelinesx.flow

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.NotUsed
import akka.stream.scaladsl._
import akka.util.ByteString
import pipelines.akkastream.PipelinesContext

/**
 * A class defining flow with context, that collects messages and propagate result with Pipeline context
 */
class DataCollector[T](
    transformer: (T) ⇒ String = (t: T) ⇒ t.toString + "/n",
    maxSize:     Long         = 512000000,
    duration:    Long         = 3600000) {

  private val formatter = DateTimeFormatter.ofPattern("'stream-'yyyy-MM-dd_HH_mm'.messages'")
  val buffer = new StringBuilder
  var length = 0L
  var previous: PipelinesContext = null
  var start = System.currentTimeMillis()
  var fname = LocalDateTime.now().format(formatter)

  def collectorFlow: Flow[(T, PipelinesContext), ((String, ByteString), PipelinesContext), NotUsed] =
    Flow[(T, PipelinesContext)].map { collector(_) }.filter(_.nonEmpty).map(_.head)

  def collector(elem: (T, PipelinesContext)): List[((String, ByteString), PipelinesContext)] = {
    val string = transformer(elem._1)
    val currentTime = System.currentTimeMillis()
    var result: List[((String, ByteString), PipelinesContext)] = Nil
    ((length + string.length) < maxSize) && ((currentTime - start) < duration) match {
      case true ⇒ // Continue collecting
      case false ⇒ // Start new
        result = List(((fname, ByteString(buffer.toString())), previous))
        buffer.clear()
        length = 0
        start = System.currentTimeMillis()
        fname = LocalDateTime.now().format(formatter)
    }
    buffer ++= string
    length = length + string.length
    previous = elem._2
    result
  }
}
