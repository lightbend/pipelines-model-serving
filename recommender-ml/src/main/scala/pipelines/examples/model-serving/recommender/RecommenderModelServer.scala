package pipelines.examples.modelserving.recommender

import pipelines.examples.modelserving.recommender.data.{ ProductPrediction, RecommenderRecord, RecommendationResult }
import com.lightbend.modelserving.model.actor.{ ModelServingActor, ModelServingManager }
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelManager, ModelType, ServingActorResolver, ServingResult }
import akka.Done
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import scala.concurrent.duration._

final case object RecommenderModelServer extends AkkaStreamlet {

  val dtype = "recommender"
  val in0 = AvroInlet[RecommenderRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[RecommendationResult]("out", _.dataType)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    val modelManager = new ModelManager[RecommenderRecord, Seq[ProductPrediction]](RecommendationFactoryResolver)
    val actor = context.system.actorOf(
      ModelServingActor.props[RecommenderRecord, Seq[ProductPrediction]](dtype, modelManager))
    val resolver = new ServingActorResolver(Map(dtype -> actor), Some(actor))

    val modelserver = context.system.actorOf(ModelServingManager.props(resolver))

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }
    protected def dataFlow =
      FlowWithPipelinesContext[RecommenderRecord].mapAsync(1) {
        data ⇒
          modelserver.ask(RecommendationDataRecord(data))
            .mapTo[ServingResult[Seq[ProductPrediction]]]
        // }.filter {
        //   r ⇒ r.result != None
      }.map {
        r ⇒ RecommendationResult(r.name, r.dataType, r.duration, r.result.get)
      }
    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelserver.ask(descriptor).mapTo[Done]
      }
  }
}

object RecommenderModelServerMain {
  def main(args: Array[String]): Unit = {

    val dtype = "recommender"
    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelManager = new ModelManager[RecommenderRecord, Seq[ProductPrediction]](RecommendationFactoryResolver)
    val actors = Map(dtype -> system.actorOf(ModelServingActor.props[RecommenderRecord, Seq[ProductPrediction]](dtype, modelManager)))

    val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))
    val descriptor = new ModelDescriptor(
      name = "Tensorflow Model",
      description = "For model Serving",
      dataType = dtype,
      modelType = ModelType.TENSORFLOWSERVING,
      modelBytes = None,
      modelSourceLocation = Some("http://recommender1-service-kubeflow.lightshift.lightbend.com/v1/models/recommender1/versions/1:predict"))

    println(s"Sending descriptor $descriptor to the scoring engine...")
    modelserver.ask(descriptor)
    val record = new RecommenderRecord(10L, Seq(1L, 2L, 3L, 4L), dtype)
    Thread.sleep(1000)
    val result = modelserver.ask(RecommendationDataRecord(record)).mapTo[ServingResult[Seq[ProductPrediction]]]
    Thread.sleep(1000)
    println(result)
  }
}
