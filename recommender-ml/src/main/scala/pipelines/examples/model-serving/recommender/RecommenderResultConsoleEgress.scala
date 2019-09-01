package pipelines.examples.modelserving.recommender

import akka.stream.scaladsl.Sink
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.AvroInlet
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.examples.modelserving.recommender.data._

final case object RecommenderResultConsoleEgress extends AkkaStreamlet {
  val inlet = AvroInlet[RecommenderResult]("in")
  final override val shape = StreamletShape.withInlets(inlet)

  override def createLogic = new RunnableGraphStreamletLogic {
    def runnableGraph =
      atMostOnceSource(inlet).to(Sink.foreach(line ⇒ println("Recommender: " + line)))
  }
}
