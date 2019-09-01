package pipelines.examples.modelserving.recommender

import pipelines.examples.modelserving.recommender.data.{ RecommenderRecord, RecommenderResult }
import pipelines.examples.modelserving.recommender.models.tensorflow.{ RecommenderTensorFlowServingModel, RecommenderTensorFlowServingModelFactory }
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelType }
import com.lightbend.modelserving.model.util.MainBase
import akka.Done
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.lightbend.modelserving.model.persistence.ModelPersistence
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.examples.modelserving.recommender.result.ModelKeyDoubleValueArrayResult
import pipelines.streamlets.{ ReadWriteMany, StreamletShape, VolumeMount }
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }

import scala.concurrent.duration._
import scala.concurrent.Await

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

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelPersist = ModelPersistence[RecommenderRecord, ModelKeyDoubleValueArrayResult](
      modelFactory = RecommenderTensorFlowServingModelFactory,
      modelName = context.streamletRef,
      baseDirPath = persistentDataMount.path)

    val modelServer = context.system.actorOf(
      ModelServingActor.props[RecommenderRecord, ModelKeyDoubleValueArrayResult](
        label = "recommender",
        modelFactory = RecommenderTensorFlowServingModelFactory,
        modelPersistence = modelPersist,
        makeDefaultModelOutput = () ⇒ RecommenderTensorFlowServingModel.makeEmptyTFPredictionResult()))

    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }

    protected def dataFlow =
      FlowWithPipelinesContext[RecommenderRecord].mapAsync(1) { record ⇒
        modelServer.ask(record).mapTo[Model.ModelReturn[ModelKeyDoubleValueArrayResult]]
          .map { modelReturn ⇒
            RecommenderResult(record, modelReturn.modelOutput, modelReturn.modelResultMetadata)
          }
      }

    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelServer.ask(descriptor).mapTo[Done]
      }
  }
}

/**
 * Test program for [[RecommenderModelServer]]. Just loads the TensorFlow Serving
 * model and uses it to score one record. So, this program focuses on ensuring
 * the logic works for any model, but doesn't exercise all the available models.
 * For testing purposes, only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.recommender._
 * RecommenderModelServerMain.main(Array("-n","3","-f","1000"))
 * ```
 */
object RecommenderModelServerMain {
  val defaultCount = 3
  val defaultFrequencyMillis = 1000.milliseconds

  def main(args: Array[String]): Unit = {
    val (count, frequency) =
      MainBase.parseArgs(args, this.getClass.getName, defaultCount, defaultFrequencyMillis)

    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelServer = system.actorOf(
      ModelServingActor.props[RecommenderRecord, ModelKeyDoubleValueArrayResult](
        "recommender",
        RecommenderTensorFlowServingModelFactory,
        () ⇒ RecommenderTensorFlowServingModel.makeEmptyTFPredictionResult()))

    val location = Some("http://recommender1-service-kubeflow.lightshift.lightbend.com/v1/models/recommender1/versions/1:predict")
    val descriptor = new ModelDescriptor(
      modelType = ModelType.TENSORFLOWSERVING,
      modelName = "Tensorflow Model",
      description = "For model Serving",
      modelBytes = Some(location.get.getBytes),
      modelSourceLocation = location)

    val record = new RecommenderRecord(10L, Seq(1L, 2L, 3L, 4L))

    for (i ← 0 until count) {
      modelServer.ask(descriptor)
      Thread.sleep(100)
      val result = Await.result(modelServer.ask(record).mapTo[RecommenderResult], 5 seconds)
      println(s"$i: result - $result")
      Thread.sleep(frequency.length)
    }
    sys.exit(0)
  }
}
