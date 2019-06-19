package pipelines.examples.ml.egress

import pipelines.streamlets.avro.AvroInlet
import pipelines.egress.LogEgress
import pipelines.examples.data._
import akka.event.Logging.{ LogLevel, InfoLevel }

final case class WineResultLoggerEgress(logLevel: LogLevel = InfoLevel) extends LogEgress {
  val prefix: String = "Wine Quality: "
  type IN = WineResult
  val in = AvroInlet[WineResult]("in")
}
