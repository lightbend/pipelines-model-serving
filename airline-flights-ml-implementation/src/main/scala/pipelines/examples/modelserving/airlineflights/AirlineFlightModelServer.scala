package pipelines.examples.modelserving.airlineflights

import models.AirlineFlightH2OModelFactory
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ Model, ModelDescriptor }
import com.lightbend.modelserving.model.h2o.H2OModel
import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.modelserving.model.persistence.FilePersistence

import scala.concurrent.duration._
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithOffsetContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.{ ReadWriteMany, StreamletShape, VolumeMount }
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import hex.genmodel.easy.prediction.BinomialModelPrediction
import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import pipelines.examples.modelserving.airlineflights.result.ModelLabelProbabilityResult

final case object AirlineFlightModelServer extends AkkaStreamlet {

  val in0 = AvroInlet[AirlineFlightRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[AirlineFlightResult]("out", _.inputRecord.uniqueCarrier)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  // Declare the volume mount: 
  private val persistentDataMount = VolumeMount("persistence-data-mount", "/data", ReadWriteMany)
  override def volumeMounts = Vector(persistentDataMount)

  implicit val askTimeout: Timeout = Timeout(30.seconds)

  /** Uses the actor system as an argument to support testing outside of the streamlet. */
  def makeModelServer(sys: ActorSystem): ActorRef = {

    sys.actorOf(
      ModelServingActor.props[AirlineFlightRecord, BinomialModelPrediction](
        "airlines", AirlineFlightH2OModelFactory, () ⇒ new BinomialModelPrediction))
  }

  override final def createLogic = new RunnableGraphStreamletLogic() {
    // Set persistence
    FilePersistence.setGlobalMountPoint(getMountedPath(persistentDataMount).toString)
    FilePersistence.setStreamletName(streamletRef)

    def runnableGraph() = {
      sourceWithOffsetContext(in1).via(modelFlow).runWith(sinkWithOffsetContext)
      sourceWithOffsetContext(in0).via(dataFlow).to(sinkWithOffsetContext(out))
    }

    val modelServer = makeModelServer(context.system)

    protected def dataFlow =
      FlowWithOffsetContext[AirlineFlightRecord].mapAsync(1) { record ⇒
        modelServer.ask(record).mapTo[Model.ModelReturn[BinomialModelPrediction]]
          .map { modelReturn ⇒
            val bmp: BinomialModelPrediction = modelReturn.modelOutput
            val (label, probability) = H2OModel.fromPrediction(bmp)
            AirlineFlightResult(
              modelResult = ModelLabelProbabilityResult(label, probability),
              modelResultMetadata = modelReturn.modelResultMetadata,
              inputRecord = record)
          }
      }

    protected def modelFlow =
      FlowWithOffsetContext[ModelDescriptor]
        .mapAsync(1) { descriptor ⇒ modelServer.ask(descriptor).mapTo[Done] }
  }
}
