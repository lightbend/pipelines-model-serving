package pipelines.examples.modelserving

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.cinnamon.akka.{ CinnamonEvent, CinnamonEvents, CinnamonMetrics }
import com.lightbend.cinnamon.metric.{ GaugeDouble, Recorder }
import com.lightbend.modelserving.model.actor.{ ModelServingActor, ModelServingManager }
import com.lightbend.modelserving.model.{ ModelToServe, ServingActorResolver, ServingResult }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.examples.data.{ ModelDescriptor, WineRecord, WineResult }
import pipelines.examples.modelserving.winemodel.{ WineDataRecord, WineFactoryResolver }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }

import scala.concurrent.duration._

final case object WineModelServer extends AkkaStreamlet {

  val in0 = AvroInlet[WineRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[WineResult]("out", _.name)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    ModelToServe.setResolver[WineRecord, Double](WineFactoryResolver)

    val actors = Map("wine" ->
      context.system.actorOf(ModelServingActor.props[WineRecord, Double]))

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
        r ⇒
          {
            val sysDurationRecorder: Recorder = CinnamonMetrics(system).createRecorder(
              "MLDurationRecorder",
              tags = Map("model" -> r.name))
            sysDurationRecorder.record(r.duration)

            val sysResultRecorder: GaugeDouble = CinnamonMetrics(system).createGaugeDouble(
              "MLResultRecorder",
              tags = Map("model" -> r.name))
            sysResultRecorder.set(r.result.get)

            println("Duration: " + r.duration)

            WineResult(r.name, r.dataType, r.duration, r.result.get)
          }
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

    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val actors = Map("wine" -> system.actorOf(ModelServingActor.props[WineRecord, Double]))

    val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))
    val record = WineRecord(.0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, "wine")
    val result = modelserver.ask(WineDataRecord(record)).mapTo[ServingResult[Double]]
    Thread.sleep(100)
    println(result)
  }
}
