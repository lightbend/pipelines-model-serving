package pipelines.examples.modelserving

import akka.Done
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.lightbend.modelserving.model.actor.{ ModelServingActor, ModelServingManager }
import com.lightbend.modelserving.model.{ ModelToServe, ServingActorResolver, ServingResult }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.examples.modelserving.recommendermodel.{ RecommendationDataRecord, RecommendationFactoryResolver }
import pipelines.examples.data.{ ModelDescriptor, MOdeltype ProductPrediction, RecommenderRecord, RecommendationResult }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import scala.concurrent.duration._

class RecommenderModelServerStreamlet extends AkkaStreamlet {

  val in0 = AvroInlet[RecommenderRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[RecommendationResult]("out", _.name)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  override final def createLogic = new RunnableGraphStreamletLogic {

    ModelToServe.setResolver[RecommenderRecord, Seq[ProductPrediction]](RecommendationFactoryResolver)

    val actors = Map("recommender" -> system.actorOf(ModelServingActor.props[RecommenderRecord, Seq[ProductPrediction]]))

    val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    def runnableGraph = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }
    protected def dataFlow =
      FlowWithPipelinesContext[RecommenderRecord].mapAsync(1) {
        data ⇒
          modelserver.ask(RecommendationDataRecord(data))
            .mapTo[ServingResult[Seq[ProductPrediction]]]
      }.filter {
        r ⇒ r.result != None
      }.map {
        r ⇒ RecommendationResult(r.name, r.dataType, r.duration, r.result.get)
      }
    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].map {
        model ⇒ ModelToServe.fromModelRecord(model)
      }.mapAsync(1) {
        model ⇒ modelserver.ask(model).mapTo[Done]
      }
  }
}

object RecommenderModelServerStreamlet {
  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val actors = Map("recommender" -> system.actorOf(ModelServingActor.props[RecommenderRecord, Seq[ProductPrediction]]))
    ModelToServe.setResolver[RecommenderRecord, Seq[ProductPrediction]](RecommendationFactoryResolver)

    val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))
    val model = new ModelDescriptor(name = "Tensorflow Model", description = "For model Serving",
      dataType = "recommender", modeltype = ModelType.TENSORFLOWSERVING, modeldata = null,
      modeldatalocation = Some("http://recommender1-service-kubeflow.lightshift.lightbend.com/v1/models/recommender1/versions/1:predict"))

    modelserver.ask(ModelToServe.fromModelRecord(model))
    val record = new RecommenderRecord(10L, Seq(1L, 2L, 3L, 4L), "recommender")
    Thread.sleep(1000)
    val result = modelserver.ask(RecommendationDataRecord(record)).mapTo[ServingResult[Seq[ProductPrediction]]]
    Thread.sleep(1000)
    println(result)
  }
}
