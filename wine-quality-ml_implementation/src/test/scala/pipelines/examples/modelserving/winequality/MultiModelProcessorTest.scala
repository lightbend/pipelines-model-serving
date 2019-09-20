package pipelines.examples.modelserving.winequality

import java.io.ByteArrayOutputStream

import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelServingStats, ModelType, MultiModelFactory }
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.winequality.data.WineRecord
import pipelines.examples.modelserving.winequality.models.pmml.WinePMMLModelFactory
import pipelines.examples.modelserving.winequality.models.tensorflow.{ WineTensorFlowBundledModelFactory, WineTensorFlowModelFactory }

class MultiModelProcessorTest extends FlatSpec {

  val name = "test name"
  val description = "test description"
  val modelstats = ModelServingStats.unknown

  val record = WineRecord("", .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0)

  val modelFactory = MultiModelFactory(
    Map(
      ModelType.PMML -> WinePMMLModelFactory,
      ModelType.TENSORFLOW -> WineTensorFlowModelFactory,
      ModelType.TENSORFLOWSAVED -> WineTensorFlowBundledModelFactory))

  "An instance PMMLWineModel" should "create correctly" in {

    createPMMLModel() match {
      case Right(model) ⇒
        // Model created successfully
        val result = model.score(record, modelstats).modelOutput
        println(s"PMML model - result $result")
        assert(result == 5.0)
        ()
      case Left(error) ⇒
        println(s"  Failed to instantiate the model: $error")
        assert(false)
    }
  }

  "An instance TensorflowWineModel" should "create correctly" in {
    createTensorFlowModel() match {
      case Right(model) ⇒
        // Model created successfully
        val result = model.score(record, modelstats).modelOutput
        println(s"Tensorflow model - result $result")
        assert(result == 5.0)
        ()
      case Left(error) ⇒
        println(s"  Failed to instantiate the model: $error")
        assert(false)
    }
  }

  "An instance TensorflowSavedWineModel" should "create correctly" in {
    createTensorFlowSavedModel() match {
      case Right(model) ⇒
        // Model created successfully
        val result = model.score(record, modelstats).modelOutput
        println(s"Tensorflow bundled model - result $result")
        assert(result == 5.0)
        ()
      case Left(error) ⇒
        println(s"  Failed to instantiate the model: $error")
        assert(false)
    }
  }

  private def createPMMLModel(): Either[String, Model[WineRecord, Double]] = {
    val is = this.getClass.getClassLoader.getResourceAsStream("wine/models/winequalityDecisionTreeClassification.pmml")
    val buffer = new Array[Byte](1024)
    val content = new ByteArrayOutputStream()
    Stream.continually(is.read(buffer)).takeWhile(_ != -1).foreach(content.write(buffer, 0, _))
    val pmml = content.toByteArray
    val model = new ModelDescriptor(
      modelName = "Wine model",
      description = "Wine PMML model",
      modelType = ModelType.PMML,
      modelBytes = Some(pmml),
      modelSourceLocation = None)
    modelFactory.create(model)
  }

  private def createTensorFlowModel(): Either[String, Model[WineRecord, Double]] = {
    val is = this.getClass.getClassLoader.getResourceAsStream("wine/models/optimized_WineQuality.pb")
    val buffer = new Array[Byte](1024)
    val content = new ByteArrayOutputStream()
    Stream.continually(is.read(buffer)).takeWhile(_ != -1).foreach(content.write(buffer, 0, _))
    val tf = content.toByteArray
    val model = new ModelDescriptor(
      modelName = "Wine model",
      description = "Wine tensorflow model",
      modelType = ModelType.TENSORFLOW,
      modelBytes = Some(tf),
      modelSourceLocation = None)
    modelFactory.create(model)
  }

  private def createTensorFlowSavedModel(): Either[String, Model[WineRecord, Double]] = {
    val url = this.getClass.getClassLoader.getResource("wine/models/saved/1/")
    val model = new ModelDescriptor(
      modelName = "Wine model",
      description = "Wine tensorflow saved model",
      modelType = ModelType.TENSORFLOWSAVED,
      modelBytes = None,
      modelSourceLocation = Some(url.getPath))
    modelFactory.create(model)
  }
}
