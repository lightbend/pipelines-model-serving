package pipelinesx.flow

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.stream.scaladsl._
import akka.util.ByteString

/**
 * Flow for packing individual records into larger array and propagate result with Pipeline context
 *
 * @param maxSize max size for collected data
 * @param duration max duration to collect
 * @param transformer:  (IN) => String = (in: IN) => in.toString + "/n"
 */

class DataCollector[T](
    transformer: (T) ⇒ String = (t: T) ⇒ t.toString + "/n",
    maxSize:     Long         = 512000000,
    duration:    Long         = 3600000) {

  private val formatter = DateTimeFormatter.ofPattern("'stream-'yyyy-MM-dd_HH_mm'.messages'")
  val buffer = new StringBuilder
  var length = 0L
  private var previous: CommittableOffset = null // Context to return
  var start = System.currentTimeMillis()
  var fname = LocalDateTime.now().format(formatter)

  def collectorFlow: Flow[(T, CommittableOffset), ((String, ByteString), CommittableOffset), NotUsed] =
    Flow[(T, CommittableOffset)].map { collector(_) }.filter(_.nonEmpty).map(_.head)

  def collector(elem: (T, CommittableOffset)): List[((String, ByteString), CommittableOffset)] = {
    val string = transformer(elem._1)
    val currentTime = System.currentTimeMillis()
    var result: List[((String, ByteString), CommittableOffset)] = Nil
    ((length + string.length) < maxSize) && ((currentTime - start) < duration) match {
      case true ⇒ // Continue collecting
      case false ⇒ // Start new
        // Create result
        result = List(((fname, ByteString(buffer.toString())), previous))
        // Reset parameters for the next collection
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
