package pipelines.examples.modelserving.airlineflights

import models.AirlineFlightH2OModelFactory
import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType, ServingResult }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
import akka.Done
import akka.actor.ActorRef
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }

final case object AirlineFlightModelServer extends AkkaStreamlet {

  val in0 = AvroInlet[AirlineFlightRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[AirlineFlightResult]("out", _.uniqueCarrier)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  implicit val askTimeout: Timeout = Timeout(30.seconds)

  def makeModelServer(): ActorRef = {
    context.system.actorOf(
      ModelServingActor.props[AirlineFlightRecord, AirlineFlightResult](
        "airlines", AirlineFlightH2OModelFactory))
  }

  override final def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }

    val modelServer = makeModelServer()

    protected def dataFlow =
      FlowWithPipelinesContext[AirlineFlightRecord].mapAsync(1) {
        record ⇒ modelServer.ask(record).mapTo[ServingResult[AirlineFlightResult]]
      }.filter {
        r ⇒ r.result != None
      }.map {
        r ⇒
          val result = r.result.get
          result.modelName = r.name
          result.modelType = r.modelType.toString
          result.duration = r.duration
          result
      }
    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelServer.ask(descriptor).mapTo[Done]
      }
  }
}

object AirlineFlightModelServerMain {
  // WARNING: Currently, the Pipelines plugin interferes with running mains,
  // even when you use
  //   runMain pipelines.examples.modelserving.airlineflights.AirlineFlightModelServerMain
  // Instead, start the console and run it there:
  // ```
  // import pipelines.examples.modelserving.airlineflights._
  // AirlineFlightModelServerMain.main(Array())
  // ...
  // ```
  def main(args: Array[String]): Unit = {

    println("Starting...")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    println("Making the model server (actor)...")
    val modelServer = AirlineFlightModelServer.makeModelServer()

    println("Getting the H2O model...")
    val is = this.getClass.getClassLoader.getResourceAsStream("airlines/models/mojo/gbm_pojo_test.zip")
    val mojo = new Array[Byte](is.available)
    is.read(mojo)

    val descriptor = new ModelDescriptor(
      name = "Airline model",
      description = "Mojo airline model",
      modelType = ModelType.H2O,
      modelBytes = Some(mojo),
      modelSourceLocation = Some("classpath:airlines/models/mojo/gbm_pojo_test.zip"))

    println(s"Sending descriptor ${descriptor.toRichString} to the scoring engine...\n")
    val modelLoadResult = Await.result(modelServer.ask(descriptor), 5 seconds)
    println(s"Result from loading the model: $modelLoadResult\n")

    val record = AirlineFlightRecord(1990, 1, 3, 3, 1707, 1630, 1755, 1723, "US", 29, 0, 48, 53, 0, 32, 37, "CMH", "IND", 182, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    println("Sending record to the scoring engine...")
    val resultFuture = modelServer.ask(record).mapTo[ServingResult[AirlineFlightResult]]
    val result = Await.result(resultFuture, 2 seconds)
    result.result match {
      case None    ⇒ println(s"Received a None in the result: $result")
      case Some(r) ⇒ println(s"Received result: $r")
    }
    println("\n\nCalling exit. If in sbt, ignore 'sbt.TrapExitSecurityException'...\n\n")
    sys.exit(0)
  }
}
