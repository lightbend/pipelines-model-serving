package pipelines.examples.modelserving

import akka.Done
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import akka.pattern.ask
import com.lightbend.modelserving.model.actor.ModelServingManager
import com.lightbend.modelserving.model.{ ModelToServe, ServingResult }
import pipelines.akkastream.{ AkkaStreamlet, StreamletContext, StreamletLogic }
import pipelines.examples.data._
import pipelines.examples.modelserving.winemodel.WineFactoryResolver
import pipelines.examples.data.DataCodecs._
import com.lightbend.modelserving.model.ModelCodecs._

import scala.concurrent.duration._
import pipelines.streamlets.{ FanIn, _ }

class ModelServerStreamlet extends AkkaStreamlet {
  override implicit val shape = new FanInOut[WineRecord, ModelDescriptor, Result]

  override final def createLogic: ModelServer = new ModelServer()
}

class ModelServer()(implicit shape: FanInOut[WineRecord, ModelDescriptor, Result], context: StreamletContext) extends StreamletLogic {

  ModelToServe.setResolver[WineRecord, Double](WineFactoryResolver)
  val in0 = atLeastOnceSource[WineRecord](shape.inlet0)
  val in1 = atLeastOnceSource[ModelDescriptor](shape.inlet1)
  val out = atLeastOnceSink[Result](shape.outlet0)

  override def init(): Unit = {
    val modelserver = system.actorOf(ModelServingManager.props[WineRecord, Double])
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    // Data stream processing
    in0.mapAsync(1)(data ⇒ modelserver.ask(data).mapTo[ServingResult[Result]])
      .filter(r ⇒ r.result != None)
      .map(r ⇒ Result(r.name, r.dataType, r.duration, r.result.asInstanceOf[Option[Double]]))
      .runWith(out)

    // Model stream processing
    in1.map(model ⇒ ModelToServe.fromModelRecord(model))
      .mapAsync(1)(model ⇒ modelserver.ask(model).mapTo[Done])
      .runWith(Sink.ignore)
  }
}

object FanInOut {
  val InletName = new IndexedPrefix("in", 2)
  val outletName = new IndexedPrefix("out", 1)
}

final class FanInOut[In0: KeyedSchema, In1: KeyedSchema, Out0: KeyedSchema] extends StreamletShape {
  val inlet0 = KeyedInletPort[In0](FanIn.inletName(0))
  val inlet1 = KeyedInletPort[In1](FanIn.inletName(1))

  val outlet0 = KeyedOutletPort[Out0](FanOut.outletName(0))

  final override def inlets = Vector(inlet0, inlet1)
  final override def outlets = Vector(outlet0)
}
