package pipelines.examples.modelserving.winequality

import pipelines.examples.modelserving.winequality.models.{ WineDataRecord, WineFactoryResolver }
import pipelines.examples.modelserving.winequality.data.{ WineRecord, WineResult }
import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.modelserving.model.actor.{ ModelServingActor, ModelServingManager }
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType, ModelToServe, ServingActorResolver, ServingResult }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{AvroInlet, AvroOutlet}

import scala.concurrent.duration._

final case object WineModelServer extends AkkaStreamlet {

  val dtype = "wine"
  val in0 = AvroInlet[WineRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[WineResult]("out", _.dataType)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    ModelToServe.setResolver[WineRecord, Double](WineFactoryResolver)

    val actors = Map(dtype ->
      context.system.actorOf(ModelServingActor.props[WineRecord, Double](dtype)))

    val modelserver = context.system.actorOf(
      ModelServingManager.props(new ServingActorResolver(actors)))

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }
    protected def dataFlow =
      FlowWithPipelinesContext[WineRecord].mapAsync(1) {
        data ⇒ modelserver.ask(WineDataRecord(data)).mapTo[ServingResult[Double]]
      }.filter {
        r ⇒ r.result != None
      }.map {
        r ⇒ WineResult(r.name, r.dataType, r.duration, r.result.get)
      }
    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].map {
        model ⇒ ModelToServe.fromModelRecord(model)
      }.mapAsync(1) {
        model ⇒ modelserver.ask(model).mapTo[Done]
      }
  }
}

object WineModelServerMain {
  def main(args: Array[String]): Unit = {

    val dtype = "wine"
    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val executor = system.getDispatcher
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    ModelToServe.setResolver[WineRecord, Double](WineFactoryResolver)

    val actors = Map(dtype -> system.actorOf(ModelServingActor.props[WineRecord, Double](dtype)))

    val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))

    val is = this.getClass.getClassLoader.getResourceAsStream("wine/models/winequalityDecisionTreeClassification.pmml")
    val pmml = new Array[Byte](is.available)
    is.read(pmml)
    val model = new ModelDescriptor(name = "Wine Model", description = "winequalityDecisionTreeClassification",
      dataType = dtype, modeltype = ModelType.PMML, modeldata = Some(pmml), modeldatalocation = None)

    modelserver.ask(ModelToServe.fromModelRecord(model))
    Thread.sleep(100)

    val record = WineRecord(.0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, dtype)
    val result = modelserver.ask(WineDataRecord(record)).mapTo[ServingResult[Double]]
    result.map(data => {
      println(s"Result ${data.result.get}, model ${data.name}, data type ${data.dataType}, duration ${data.duration}")
    })
  }
}
