package pipelines.examples.ml.egress

import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelines.egress.LogEgressLogic
import pipelines.examples.data._
import akka.event.Logging.{ LogLevel, InfoLevel }

final case object WineResultLoggerEgress extends AkkaStreamlet {
  val in = AvroInlet[WineResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  override def createLogic = LogEgressLogic.makeFromConfig[WineResult](
    in = in,
    logLevelConfigKey = "wine-quality.log-level",
    prefix = "Wine Quality:")
}

