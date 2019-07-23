package pipelines.examples.modelserving.winequality

import pipelines.examples.modelserving.winequality.models.WineDataRecord
import pipelines.examples.modelserving.winequality.data.{ WineRecord, WineResult }
import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.modelserving.model.actor.{ ModelServingActor, ModelServingManager }
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType, MultiModelFactory, ServingResult }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }

import scala.concurrent.duration._

final case object WineModelServer extends AkkaStreamlet {

  val in0 = AvroInlet[WineRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[WineResult]("out", _.name)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  val modelFactory = MultiModelFactory(
    Map(
      ModelType.PMML -> WinePMMLModel,
      ModelType.TENSORFLOW -> WineTensorFlowModel,
      ModelType.TENSORFLOWSAVED -> WineTensorFlowBundledModel))

  override final def createLogic = new RunnableGraphStreamletLogic() {

    val modelserver = context.system.actorOf(
      ModelServingActor.props[WineRecord, Double]("wine", modelFactory))

    // val resolver = new ServingActorResolver(Map("wine" -> actor), Some(actor))
    // val modelserver = context.system.actorOf(ModelServingManager.props(resolver))

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
        r ⇒ WineResult(r.name, r.duration, r.result.get)
      }
    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelserver.ask(descriptor).mapTo[Done]
      }
  }
}

object WineModelServerMain {
  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val executor = system.getDispatcher
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelserver = system.actorOf(
      ModelServingActor.props[WineRecord, Double]("wine", WineModelServer.modelFactory))

    // val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))

    val path = "wine/models/winequalityDecisionTreeClassification.pmml"
    val is = this.getClass.getClassLoader.getResourceAsStream(path)
    val pmml = new Array[Byte](is.available)
    is.read(pmml)
    val descriptor = new ModelDescriptor(
      name = "Wine Model",
      description = "winequalityDecisionTreeClassification",
      modelType = ModelType.PMML,
      modelBytes = Some(pmml),
      modelSourceLocation = None)

    modelserver.ask(descriptor)
    Thread.sleep(100)

    val record = WineRecord(
      "wine quality sample data",
      .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0)
    val result = modelserver.ask(WineDataRecord(record)).mapTo[ServingResult[Double]]
    result.foreach(data ⇒ {
      println(s"Result ${data.result.get}, model ${data.name}, duration ${data.duration}")
    })
  }
}
