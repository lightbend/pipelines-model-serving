package pipelines.examples.modelserving

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.modelserving.model.actor.{ ModelServingActor, ModelServingManager }
import com.lightbend.modelserving.model.{ ModelToServe, ServingActorResolver, ServingResult }
import pipelines.akkastream.{ AkkaStreamlet, StreamletContext, StreamletLogic }
import pipelines.examples.data._
import pipelines.examples.modelserving.winemodel.{ WineDataRecord, WineFactoryResolver }

import scala.concurrent.duration._
import pipelines.streamlets.{ FanIn, _ }

class WineModelServerStreamlet extends AkkaStreamlet {

  override implicit val shape = new WineFanInOut[WineRecord, ModelDescriptor, WineResult]

  override final def createLogic: WineModelServer = new WineModelServer()
}

class WineModelServer()(implicit shape: WineFanInOut[WineRecord, ModelDescriptor, WineResult], context: StreamletContext) extends StreamletLogic {

  ModelToServe.setResolver[WineRecord, Double](WineFactoryResolver)
  val in0 = atLeastOnceSource[WineRecord](shape.inlet0)
  val in1 = atLeastOnceSource[ModelDescriptor](shape.inlet1)
  val out = atLeastOnceSink[WineResult](shape.outlet0)

  override def init(): Unit = {

    val actors = Map("wine" -> system.actorOf(ModelServingActor.props[WineRecord, Double]))

    val modelserver = system.actorOf(ModelServingManager.props(new ServingActorResolver(actors)))
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    // Data stream processing
    in0.mapAsync(1)(data ⇒ modelserver.ask(WineDataRecord(data)).mapTo[ServingResult[Double]])
      .filter(r ⇒ r.result != None)
      .map(r ⇒ WineResult(r.name, r.dataType, r.duration, r.result.get))
      .runWith(out)

    // Model stream processing
    in1.map(model ⇒ ModelToServe.fromModelRecord(model))
      .mapAsync(1)(model ⇒ modelserver.ask(model).mapTo[Done])
      .runWith(Sink.ignore)
  }
}

object WineFanInOut {
  val InletName = new IndexedPrefix("in", 2)
  val outletName = new IndexedPrefix("out", 1)
}

final class WineFanInOut[In0: KeyedSchema, In1: KeyedSchema, Out0: KeyedSchema] extends StreamletShape {
  val inlet0 = KeyedInletPort[In0](FanIn.inletName(0))
  val inlet1 = KeyedInletPort[In1](FanIn.inletName(1))

  val outlet0 = KeyedOutletPort[Out0](FanOut.outletName(0))

  final override def inlets = Vector(inlet0, inlet1)
  final override def outlets = Vector(outlet0)
}

object WineModelServer {
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
