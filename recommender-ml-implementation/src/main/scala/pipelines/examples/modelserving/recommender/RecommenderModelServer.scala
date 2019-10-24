package pipelines.examples.modelserving.recommender

import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ Model, ModelDescriptor }
import akka.Done
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.modelserving.model.persistence.FilePersistence
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithOffsetContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.{ ReadWriteMany, StreamletShape, VolumeMount }
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import pipelines.examples.modelserving.recommender.data.{ RecommenderRecord, RecommenderResult }
import pipelines.examples.modelserving.recommender.models.tensorflow.{ RecommenderTensorFlowServingModel, RecommenderTensorFlowServingModelFactory }
import pipelines.examples.modelserving.recommender.result.ModelKeyDoubleValueArrayResult

import scala.concurrent.duration._

final case object RecommenderModelServer extends AkkaStreamlet {

  val dtype = "recommender"
  val in0 = AvroInlet[RecommenderRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[RecommenderResult]("out", _.inputRecord.user.toString)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  // Declare the volume mount: 
  private val persistentDataMount = VolumeMount("persistence-data-mount", "/data", ReadWriteMany)
  override def volumeMounts = Vector(persistentDataMount)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelserver = context.system.actorOf(
      ModelServingActor.props[RecommenderRecord, ModelKeyDoubleValueArrayResult](
        "recommender",
        RecommenderTensorFlowServingModelFactory,
        () ⇒ RecommenderTensorFlowServingModel.makeEmptyTFPredictionResult()))

    def runnableGraph() = {
      // Set persistence
      FilePersistence.setGlobalMountPoint(getMountedPath(persistentDataMount).toString)
      FilePersistence.setStreamletName(streamletRef)

      sourceWithOffsetContext(in1).via(modelFlow).runWith(sinkWithOffsetContext)
      sourceWithOffsetContext(in0).via(dataFlow).to(sinkWithOffsetContext(out))
    }

    protected def dataFlow =
      FlowWithOffsetContext[RecommenderRecord].mapAsync(1) { record ⇒
        modelserver.ask(record).mapTo[Model.ModelReturn[ModelKeyDoubleValueArrayResult]]
          .map { modelReturn ⇒
            RecommenderResult(record, modelReturn.modelOutput, modelReturn.modelResultMetadata)
          }
      }

    protected def modelFlow =
      FlowWithOffsetContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelserver.ask(descriptor).mapTo[Done]
      }
  }
}
