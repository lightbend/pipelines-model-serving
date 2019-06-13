package pipelines.examples.modelserving

import akka.Done
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.lightbend.modelserving.model.actor.{ ModelServingActor, ModelServingManager }
import com.lightbend.modelserving.model.{ ModelToServe, ServingActorResolver, ServingResult }
import pipelines.akkastream.{ AkkaStreamlet, StreamletContext, StreamletLogic }
import pipelines.examples.modelserving.recommendermodel.{ RecommendationDataRecord, RecommendationFactoryResolver }
import pipelines.examples.data._
import pipelines.streamlets._
import pipelines.streamlets.avro._

import scala.concurrent.duration._

class RecommenderModelServerStreamlet extends AkkaStreamlet {

  val in0 = AvroInlet[RecommenderRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[RecommendationResult]("out")
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  override final def createLogic: RecommenderModelServer =
    new RecommenderModelServer(shape)
}

class RecommenderModelServer(shape: StreamletShape)
  extends StreamletLogic {

  ModelToServe.setResolver[RecommenderRecord, Seq[ProductPrediction]](RecommendationFactoryResolver)
  val in0 = atLeastOnceSource[RecommenderRecord](shape.inlet0)
  val in1 = atLeastOnceSource[ModelDescriptor](shape.inlet1)
  val out = atLeastOnceSink[RecommendationResult](shape.outlet0)

  override def init(): Unit = {

    val actors = Map("recommender" -> system.actorOf(ModelServingActor.props[RecommenderRecord, Seq[ProductPrediction]]))

    val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    // Data stream processing
    // DeanW: It doesn't appear that ModelServingManager expects RecommendationDataRecord messages!
    in0.mapAsync(1)(data ⇒ modelserver.ask(RecommendationDataRecord(data)).mapTo[ServingResult[Seq[ProductPrediction]]])
      .filter(r ⇒ r.result != None)
      .map(r ⇒ new RecommendationResult(r.name, r.dataType, r.duration, r.result.get))
      .runWith(out)

    // Model stream processing
    in1.map(model ⇒ ModelToServe.fromModelRecord(model))
      .mapAsync(1)(model ⇒ modelserver.ask(model).mapTo[Done])
      .runWith(Sink.ignore)
  }
}

object RecommenderModelServer {
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
