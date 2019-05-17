package pipelines.examples.modelserving

import akka.Done
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.lightbend.modelserving.model.ModelCodecs._
import com.lightbend.modelserving.model.actor.{ ModelServingActor, ModelServingManager }
import com.lightbend.modelserving.model.{ ModelToServe, ServingActorResolver, ServingResult }
import pipelines.akkastream.{ AkkaStreamlet, StreamletContext, StreamletLogic }
import pipelines.examples.data.DataCodecs._
import pipelines.examples.modelserving.recommedermodel.{ RecommendationDataRecord, RecommendationFactoryResolver }
import pipelines.examples.data._
import pipelines.streamlets.{ FanIn, _ }

import scala.concurrent.duration._

class RecommenderModelServerStreamlet extends AkkaStreamlet {

  override implicit val shape = new RecommenderFanInOut[RecommenderRecord, ModelDescriptor, RecommendationResult]

  override final def createLogic: RecommenderModelServer = new RecommenderModelServer()
}

class RecommenderModelServer()(implicit shape: RecommenderFanInOut[RecommenderRecord, ModelDescriptor, RecommendationResult], context: StreamletContext) extends StreamletLogic {

  ModelToServe.setResolver[RecommenderRecord, Seq[ProductPrediction]](RecommendationFactoryResolver)
  val in0 = atLeastOnceSource[RecommenderRecord](shape.inlet0)
  val in1 = atLeastOnceSource[ModelDescriptor](shape.inlet1)
  val out = atLeastOnceSink[RecommendationResult](shape.outlet0)

  override def init(): Unit = {

    val actors = Map("recommender" -> system.actorOf(ModelServingActor.props[RecommenderRecord, Seq[ProductPrediction]]))

    val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    // Data stream processing
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

object RecommenderFanInOut {
  val InletName = new IndexedPrefix("in", 2)
  val outletName = new IndexedPrefix("out", 1)
}

final class RecommenderFanInOut[In0: KeyedSchema, In1: KeyedSchema, Out0: KeyedSchema] extends StreamletShape {
  val inlet0 = KeyedInletPort[In0](FanIn.inletName(0))
  val inlet1 = KeyedInletPort[In1](FanIn.inletName(1))

  val outlet0 = KeyedOutletPort[Out0](FanOut.outletName(0))

  final override def inlets = Vector(inlet0, inlet1)
  final override def outlets = Vector(outlet0)
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
