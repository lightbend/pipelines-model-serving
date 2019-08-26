package pipelines.examples.modelserving.winequality

import pipelines.examples.modelserving.winequality.data.{ WineRecord, WineResult }
import models.pmml.WinePMMLModelFactory
import models.tensorflow.{ WineTensorFlowBundledModelFactory, WineTensorFlowModelFactory }
import akka.Done
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.{ ReadWriteMany, StreamletShape, VolumeMount }
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelType, MultiModelFactory }
import com.lightbend.modelserving.model.persistence.FilePersistence
import pipelines.examples.modelserving.winequality.result.ModelDoubleResult

final case object WineModelServer extends AkkaStreamlet {

  val in0 = AvroInlet[WineRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[WineResult]("out", _.inputRecord.lot_id)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  // Declare the volume mount: 
  private val persistentDataMount =
    VolumeMount("persistence-data-mount", "/data", ReadWriteMany)
  override def volumeMounts = Vector(persistentDataMount)
  FilePersistence.setGlobalMountPoint(persistentDataMount.path)

  val modelFactory = MultiModelFactory(
    Map(
      ModelType.PMML -> WinePMMLModelFactory,
      ModelType.TENSORFLOW -> WineTensorFlowModelFactory,
      ModelType.TENSORFLOWSAVED -> WineTensorFlowBundledModelFactory))

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    FilePersistence.setStreamletName(context.streamletRef)
    val modelserver = context.system.actorOf(
      ModelServingActor.props[WineRecord, Double](
        "wine",
        modelFactory,
        () ⇒ 0.0))

    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).to(atLeastOnceSink)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }

    protected def dataFlow =
      FlowWithPipelinesContext[WineRecord].mapAsync(1) { record ⇒
        modelserver.ask(record).mapTo[Model.ModelReturn[Double]]
          .map { modelReturn ⇒
            val result = ModelDoubleResult(value = modelReturn.modelOutput)
            WineResult(record, result, modelReturn.modelResultMetadata)
          }
      }

    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelserver.ask(descriptor).mapTo[Done]
      }
  }
}
