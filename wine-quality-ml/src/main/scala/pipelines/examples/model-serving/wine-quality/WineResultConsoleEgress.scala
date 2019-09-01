package pipelines.examples.modelserving.winequality

import akka.stream.scaladsl.Sink
import pipelines.examples.modelserving.winequality.data._
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.akkastream.AkkaStreamlet

final case object WineResultConsoleEgress extends AkkaStreamlet {
  val inlet = AvroInlet[WineResult]("in")
  final override val shape = StreamletShape.withInlets(inlet)

  override def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      atMostOnceSource(inlet).to(Sink.foreach(line ⇒ println("Wine Quality: " + line)))
  }
}

