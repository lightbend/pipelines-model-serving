package pipelines.examples.model

import java.io.ByteArrayOutputStream

import akka.actor.ActorSystem
import akka.util.Timeout
import akka.pattern.ask
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelType, MultiModelFactory }
import com.lightbend.modelserving.model.actor.ModelServingActor
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.winequality.data.WineRecord
import pipelines.examples.modelserving.winequality.models.pmml.WinePMMLModelFactory
import pipelines.examples.modelserving.winequality.models.tensorflow.{ WineTensorFlowBundledModelFactory, WineTensorFlowModelFactory }

import scala.concurrent.duration._

class WineProcessorCompleteTest extends FlatSpec {

  implicit val system: ActorSystem = ActorSystem("ModelServing")
  implicit val executor = system.getDispatcher
  implicit val askTimeout = Timeout(30.seconds)
  val record = WineRecord("", .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0)
  val modelFactory = MultiModelFactory(
    Map(
      ModelType.PMML -> WinePMMLModelFactory,
      ModelType.TENSORFLOW -> WineTensorFlowModelFactory,
      ModelType.TENSORFLOWSAVED -> WineTensorFlowBundledModelFactory))

  private def getPMMLModel(): ModelDescriptor = {
    val is = this.getClass.getClassLoader.getResourceAsStream("wine/models/winequalityDecisionTreeClassification.pmml")
    val buffer = new Array[Byte](1024)
    val content = new ByteArrayOutputStream()
    Stream.continually(is.read(buffer)).takeWhile(_ != -1).foreach(content.write(buffer, 0, _))
    val pmml = content.toByteArray
    new ModelDescriptor(
      modelName = "Wine model",
      description = "Wine PMML model",
      modelType = ModelType.PMML,
      modelBytes = Some(pmml),
      modelSourceLocation = None)
  }

  private def getTensorflowModel(): ModelDescriptor = {
    val is = this.getClass.getClassLoader.getResourceAsStream("wine/models/optimized_WineQuality.pb")
    val buffer = new Array[Byte](1024)
    val content = new ByteArrayOutputStream()
    Stream.continually(is.read(buffer)).takeWhile(_ != -1).foreach(content.write(buffer, 0, _))
    val pmml = content.toByteArray
    new ModelDescriptor(
      modelName = "Wine model",
      description = "Wine Tensorflow model",
      modelType = ModelType.TENSORFLOW,
      modelBytes = Some(pmml),
      modelSourceLocation = None)
  }

  private def getTensorflowBundeledModel(): ModelDescriptor = {
    val url = this.getClass.getClassLoader.getResource("wine/models/saved/1/")
    new ModelDescriptor(
      modelName = "Wine model",
      description = "Wine tensorflow saved model",
      modelType = ModelType.TENSORFLOWSAVED,
      modelBytes = None,
      modelSourceLocation = Some(url.getPath))
  }

  "Processing of Wine PMML Model" should "return value of 5.0" in {

    val modelserver = system.actorOf(
      ModelServingActor.props[WineRecord, Double](
        "wine", modelFactory, () ⇒ 0.0))

    // Wait for the actor to initialize and restore
    Thread.sleep(3000)

    modelserver.ask(getPMMLModel())
    println("Done setting up PMML model")
    // Wait for the model to initialize
    Thread.sleep(3000)
    modelserver.ask(record).mapTo[Model.ModelReturn[Double]]
      .map(data ⇒ {
        val result = data.modelOutput
        println(s"Executed PMML wine in ${data.modelServingStats.duration} ms with result $result")
        assert(result == 5.0)
        ()
      })
    Thread.sleep(3000)
  }

  "Processing of Wine Tensorflow Model" should "return value of 5.0" in {

    val modelserver = system.actorOf(
      ModelServingActor.props[WineRecord, Double](
        "wine", modelFactory, () ⇒ 0.0))

    // Wait for the actor to initialize and restore
    Thread.sleep(3000)

    modelserver.ask(getTensorflowModel())
    println("Done setting up Tensorflow model")
    // Wait for the model to initialize
    Thread.sleep(3000)
    modelserver.ask(record).mapTo[Model.ModelReturn[Double]]
      .map(data ⇒ {
        val result = data.modelOutput
        println(s"Executed Tensorflow wine in ${data.modelServingStats.duration} ms with result $result")
        assert(result == 5.0)
        ()
      })
    Thread.sleep(5000)
  }

  "Processing of Wine Tensorflow bundeled Model" should "return value of 5.0" in {

    val modelserver = system.actorOf(
      ModelServingActor.props[WineRecord, Double](
        "wine", modelFactory, () ⇒ 0.0))

    // Wait for the actor to initialize and restore
    Thread.sleep(3000)

    modelserver.ask(getTensorflowBundeledModel())
    println("Done setting up Tensorflow bundeled model")
    // Wait for the model to initialize
    Thread.sleep(3000)
    modelserver.ask(record).mapTo[Model.ModelReturn[Double]]
      .map(data ⇒ {
        val result = data.modelOutput
        println(s"Executed Tensorflow bundeled wine in ${data.modelServingStats.duration} ms with result $result")
        assert(result == 5.0)
        ()
      })
    Thread.sleep(5000)
  }
}
