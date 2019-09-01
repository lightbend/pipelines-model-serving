package pipelines.examples.modelserving.winequality

import pipelines.examples.modelserving.winequality.data.{ WineRecord, WineResult }
import models.pmml.WinePMMLModelFactory
import models.tensorflow.{ WineTensorFlowBundledModelFactory, WineTensorFlowModelFactory }
import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.pattern.ask
import akka.util.Timeout

import java.io.File
import scala.concurrent.duration._
import scala.concurrent.Await
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.{ ReadWriteMany, StreamletShape, VolumeMount }
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelType, MultiModelFactory }
import com.lightbend.modelserving.model.util.MainBase
import com.lightbend.modelserving.model.persistence.ModelPersistence
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

  val modelFactory = MultiModelFactory(
    Map(
      ModelType.PMML -> WinePMMLModelFactory,
      ModelType.TENSORFLOW -> WineTensorFlowModelFactory,
      ModelType.TENSORFLOWSAVED -> WineTensorFlowBundledModelFactory))

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelPersist = ModelPersistence[WineRecord, Double](
      modelName = context.streamletRef,
      modelFactory = modelFactory,
      baseDirPath = new File(persistentDataMount.path))

    val modelServer = context.system.actorOf(
      ModelServingActor.props[WineRecord, Double](
        label = "wine-quality",
        modelFactory = modelFactory,
        modelPersistence = modelPersist,
        makeDefaultModelOutput = () ⇒ 0.0))

    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }

    protected def dataFlow =
      FlowWithPipelinesContext[WineRecord].mapAsync(1) { record ⇒
        modelServer.ask(record).mapTo[Model.ModelReturn[Double]]
          .map { modelReturn ⇒
            val result = ModelDoubleResult(value = modelReturn.modelOutput)
            WineResult(record, result, modelReturn.modelResultMetadata)
          }
      }

    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelServer.ask(descriptor).mapTo[Done]
      }
  }
}

/**
 * Test program for [[WineModelServer]]. Just loads the PMML model and uses it
 * to score one record. So, this program focuses on ensuring the logic works
 * for any model, but doesn't exercise all the available models.
 * For testing purposes, only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.winequality._
 * WineModelServerMain.main(Array("-n","3","-f","1000"))
 * ```
 */
object WineModelServerMain {
  val defaultCount = 3
  val defaultFrequencyMillis = 1000.milliseconds

  def main(args: Array[String]): Unit = {
    val (count, frequency) =
      MainBase.parseArgs(args, this.getClass.getName, defaultCount, defaultFrequencyMillis)

    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelPersist = ModelPersistence[WineRecord, Double](
      modelName = "wine-quality",
      modelFactory = WineModelServer.modelFactory,
      baseDirPath = new File("./persistence"))

    val modelServer = system.actorOf(
      ModelServingActor.props[WineRecord, Double](
        "wine-quality",
        WineModelServer.modelFactory,
        modelPersist,
        () ⇒ 0.0))

    val path = "wine/models/winequalityDecisionTreeClassification.pmml"
    val is = this.getClass.getClassLoader.getResourceAsStream(path)
    val pmml = new Array[Byte](is.available)
    is.read(pmml)
    val descriptor = new ModelDescriptor(
      modelType = ModelType.PMML,
      modelName = "Wine Model",
      description = "winequalityDecisionTreeClassification",
      modelBytes = Some(pmml),
      modelSourceLocation = None)

    val record = WineRecord(
      "wine quality sample data",
      .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0)

    for (i ← 0 until count) {
      modelServer.ask(descriptor)
      Thread.sleep(100)
      val result = Await.result(modelServer.ask(record).mapTo[Model.ModelReturn[Double]], 5 seconds)
      println(s"$i: result - $result")
      Thread.sleep(frequency.length)
    }
    sys.exit(0)
  }
}
