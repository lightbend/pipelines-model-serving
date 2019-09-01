package pipelines.examples.modelserving.winequality

import java.io.ByteArrayOutputStream

import pipelinesx.modelserving.model.{ Model, ModelDescriptor, ModelServingStats, ModelType }
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.winequality.data.WineRecord
import pipelines.examples.modelserving.winequality.models.pmml.WinePMMLModelFactory

class PMMLProcessorTest extends FlatSpec {

  val name = "test name"
  val description = "test description"
  val modelstats = ModelServingStats.unknown

  val record = WineRecord("", .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0)

  "An instance PMMLWineModel" should "create correctly" in {

    createModel() match {
      case Right(model) ⇒
        // Model created successfully
        val result = model.score(record, modelstats).modelOutput
        assert(result == 5.0)
        ()
      case Left(error) ⇒
        println(s"  Failed to instantiate the model: $error")
        assert(false)
    }
  }

  private def createModel(): Either[String, Model[WineRecord, Double]] = {
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
    WinePMMLModelFactory.create(model)
  }
}
