package pipelines.examples.modelserving.airlineflights

import models.AirlineFlightH2OModelFactory
import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType, ServingResult }
import com.lightbend.modelserving.model.util.MainBase

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
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

  /** Uses the actor system as an argument to support testing outside of the streamlet. */
  def makeModelServer(sys: ActorSystem): ActorRef = {
    sys.actorOf(
      ModelServingActor.props[AirlineFlightRecord, AirlineFlightResult](
        "airlines", AirlineFlightH2OModelFactory))
  }

  override final def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }

    val modelServer = makeModelServer(context.system)

    protected def dataFlow =
      FlowWithPipelinesContext[AirlineFlightRecord].mapAsync(1) {
        record ⇒ modelServer.ask(record).mapTo[ServingResult[AirlineFlightResult]]
      }.filter {
        sr ⇒ sr.result != None // should only happen when there is no model for scoring
      }.map {
        sr ⇒ sr.result.get
      }

    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelServer.ask(descriptor).mapTo[Done]
      }
  }
}

/**
 * Test program for [[AirlineFlightModelServer]]. Just loads the PMML model and uses it
 * to score one record. So, this program focuses on ensuring the logic works
 * for any model, but doesn't exercise all the available models.
 * For testing purposes, only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.airlineflights._
 * AirlineFlightModelServerMain.main(Array("-n","3","-f","1000"))
 * ```
 */
object AirlineFlightModelServerMain {
  val defaultCount = 3
  val defaultFrequencyMillis = 1000.milliseconds

  def main(args: Array[String]): Unit = {
    val (count, frequency) =
      MainBase.parseArgs(args, this.getClass.getName, defaultCount, defaultFrequencyMillis)

    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelserver = AirlineFlightModelServer.makeModelServer(system)

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

    val record = AirlineFlightRecord(1990, 1, 3, 3, 1707, 1630, 1755, 1723, "US", 29, 0, 48, 53, 0, 32, 37, "CMH", "IND", 182, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    for (i ← 0 until count) {
      modelserver.ask(descriptor)
      Thread.sleep(100)
      val result = Await.result(modelserver.ask(record).mapTo[ServingResult[AirlineFlightResult]], 5 seconds)
      println(s"$i: result - $result")
      Thread.sleep(frequency.length)
    }
    sys.exit(0)
  }
}
