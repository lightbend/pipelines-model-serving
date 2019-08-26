package pipelines.examples.modelserving.recommender

import pipelines.examples.modelserving.recommender.data.{ RecommenderRecord, RecommenderResult }
import pipelines.examples.modelserving.recommender.models.tensorflow.{ RecommenderTensorFlowServingModel, RecommenderTensorFlowServingModelFactory }
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ Model, ModelDescriptor }
import akka.Done
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.modelserving.model.persistence.FilePersistence
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.examples.modelserving.recommender.result.ModelKeyDoubleValueArrayResult
import pipelines.streamlets.{ ReadWriteMany, StreamletShape, VolumeMount }
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }

import scala.concurrent.duration._

final case object RecommenderModelServer extends AkkaStreamlet {

  val dtype = "recommender"
  val in0 = AvroInlet[RecommenderRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[RecommenderResult]("out", _.inputRecord.user.toString)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  // Declare the volume mount: 
  private val persistentDataMount =
    VolumeMount("persistence-data-mount", "/data", ReadWriteMany)
  override def volumeMounts = Vector(persistentDataMount)
  FilePersistence.setGlobalMountPoint(persistentDataMount.path)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    FilePersistence.setStreamletName(context.streamletRef)
    val modelserver = context.system.actorOf(
      ModelServingActor.props[RecommenderRecord, ModelKeyDoubleValueArrayResult](
        "recommender",
        RecommenderTensorFlowServingModelFactory,
        () ⇒ RecommenderTensorFlowServingModel.makeEmptyTFPredictionResult()))

    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(atLeastOnceSink)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }

    protected def dataFlow =
      FlowWithPipelinesContext[RecommenderRecord].mapAsync(1) { record ⇒
        modelserver.ask(record).mapTo[Model.ModelReturn[ModelKeyDoubleValueArrayResult]]
          .map { modelReturn ⇒
            RecommenderResult(record, modelReturn.modelOutput, modelReturn.modelResultMetadata)
          }
      }

    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelserver.ask(descriptor).mapTo[Done]
      }
  }
}
