package pipelines.examples.modelserving.speculative

import akka.Done
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.persistence.FilePersistence
import com.lightbend.modelserving.model.{Model, ModelDescriptor, ModelType, MultiModelFactory}
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{FlowWithPipelinesContext, RunnableGraphStreamletLogic}
import pipelines.streamlets.avro.{AvroInlet, AvroOutlet}
import pipelines.streamlets.{ReadWriteMany, StreamletShape, VolumeMount}
import pipelines.examples.modelserving.winequality.data.{WineRecord, WineResult}
import pipelines.examples.modelserving.winequality.models.pmml.WinePMMLModelFactory
import pipelines.examples.modelserving.winequality.models.tensorflow.{WineTensorFlowBundledModelFactory, WineTensorFlowModelFactory}
import pipelines.examples.modelserving.winequality.result.ModelDoubleResult
import pipelines.examples.modelserving.winequality.speculative.{WineRecordRun, WineResultRun}

import scala.concurrent.duration._

final case object SpeculativeWineModelServer extends AkkaStreamlet {

  val in0 = AvroInlet[WineRecordRun]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[WineResultRun]("out", _.result.inputRecord.lot_id)

  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  // Declare the volume mount: 
  private val persistentDataMount =
    VolumeMount("persistence-data-mount", "/data", ReadWriteMany)
  override def volumeMounts = Vector(persistentDataMount)

  val modelFactory = MultiModelFactory(
    Map(
      ModelType.PMML -> WinePMMLModelFactory,
      ModelType.TENSORFLOW -> WineTensorFlowModelFactory,
      ModelType.TENSORFLOWSAVED -> WineTensorFlowBundledModelFactory))

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)
    // Set persistence

    FilePersistence.setGlobalMountPoint(context.getMountedPath(persistentDataMount).toString)
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
      FlowWithPipelinesContext[WineRecordRun].mapAsync(1) { record ⇒
        modelserver.ask(record.inputRecord).mapTo[Model.ModelReturn[Double]]
          .map { modelReturn ⇒
            val result = ModelDoubleResult(value = modelReturn.modelOutput)
            WineResultRun(record.uuid, WineResult(record.inputRecord, result, modelReturn.modelResultMetadata))
          }
      }

    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelserver.ask(descriptor).mapTo[Done]
      }
  }
}
