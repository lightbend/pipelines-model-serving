package pipelines.examples.modelserving.airlineflights

import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import pipelines.examples.modelserving.airlineflights.models.{ AirlineFlightDataRecord, AirlineFlightFactoryResolver }
import com.lightbend.modelserving.model.actor.{ ModelServingActor, ModelServingManager }
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelManager, ModelType, ModelToServe, ServingActorResolver, ServingResult }
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

final case object AirlineFlightModelServer extends AkkaStreamlet {

  val dtype = "airline"
  val in0 = AvroInlet[AirlineFlightRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[AirlineFlightResult]("out", _.dataType)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    val modelManager = new ModelManager[AirlineFlightRecord, AirlineFlightResult](AirlineFlightsFactoryResolver)

    val actors = Map(dtype ->
      context.system.actorOf(
        ModelServingActor.props[AirlineFlightRecord, AirlineFlightResult](dtype, modelManager)))

    val modelserver = context.system.actorOf(
      ModelServingManager.props(new ServingActorResolver(actors)))

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }
    protected def dataFlow =
      FlowWithPipelinesContext[AirlineFlightRecord].mapAsync(1) {
        data =>
          modelserver.ask(AirlineFlightDataRecord(data))
            .mapTo[ServingResult[AirlineFlightResult]]
      }.filter {
        r => r.result != None
      }.map {
        r =>
          val result = r.result.get
          result.modelname = r.name
          result.dataType = r.dataType
          result.duration = r.duration
          result
      }
    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].map {
        model ⇒ modelManager.fromModelRecord(model)
      }.mapAsync(1) {
        model ⇒ modelserver.ask(model).mapTo[Done]
      }
  }
}

object AirlineFlightModelServerMain {
  def main(args: Array[String]): Unit = {

    val dtype = "airline"
    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val executor = system.getDispatcher
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelManager = new ModelManager[AirlineFlightRecord, AirlineFlightResult](AirlineFlightsFactoryResolver)
    val actors = Map(dtype -> system.actorOf(ModelServingActor.props[AirlineFlightRecord, AirlineFlightResult](dtype, modelManager)))

    val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))
    val is = this.getClass.getClassLoader.getResourceAsStream("airlines/models/mojo/gbm_pojo_test.zip")
    val mojo = new Array[Byte](is.available)
    is.read(mojo)

    val model = new ModelDescriptor(name = "Airline model", description = "Mojo airline model",
      dataType = dtype, modeltype = ModelType.H2O, modeldata = Some(mojo), modeldatalocation = None)

    modelserver.ask(ModelToServe.fromModelRecord(model))
    val record = AirlineFlightRecord(1990, 1, 3, 3, 1707, 1630, 1755, 1723, "US", 29, 0, 48, 53, 0, 32, 37, "CMH", "IND", 182, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, dtype)
    Thread.sleep(1000)
    val result = modelserver.ask(AirlineFlightDataRecord(record)).mapTo[ServingResult[AirlineFlightResult]]
    result.map(data => {
      val r = data.result.get
      r.modelname = data.name
      r.dataType = data.dataType
      r.duration = data.duration
      println(r)
    })
  }
}
