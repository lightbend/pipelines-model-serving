package pipelines.examples.modelserving.winequality.models.tensorflow

import com.lightbend.modelserving.model.tensorflow.TensorFlowModel
import com.lightbend.modelserving.model.{ Model, ModelFactory }
import com.lightbend.modelserving.model.ModelToServe
import org.tensorflow.Tensor
import pipelines.examples.modelserving.winequality.data.WineRecord

/**
 * TensorFlow model implementation for wine data
 */
class WineTensorFlowModel(inputStream: Array[Byte]) extends TensorFlowModel[WineRecord, Double](inputStream) {

  import WineTensorFlowModel._

  /** Score a wine record with the model */
  override def score(input: WineRecord): Either[String, Double] = try {

    // Create input tensor
    val modelInput = toTensor(input)
    // Serve model using TensorFlow APIs
    val result = session.runner.feed("dense_1_input", modelInput).fetch("dense_3/Sigmoid").run().get(0)
    // Get result shape
    val rshape = result.shape
    // Map output tensor to shape
    val rMatrix = Array.ofDim[Float](rshape(0).asInstanceOf[Int], rshape(1).asInstanceOf[Int])
    result.copyTo(rMatrix)
    // Get result
    Right(rMatrix(0).indices.maxBy(rMatrix(0)).toDouble)
  } catch {
    case scala.util.control.NonFatal(th) =>
      Left("WineTensorFlowModel.score failed: $th")
  }
}

/** Factory for wine data PMML model */
object WineTensorFlowModel extends ModelFactory[WineRecord, Double] {

  def toTensor(record: WineRecord): Tensor[_] = {
    val data = Array(
      record.fixed_acidity.toFloat,
      record.volatile_acidity.toFloat,
      record.citric_acid.toFloat,
      record.residual_sugar.toFloat,
      record.chlorides.toFloat,
      record.free_sulfur_dioxide.toFloat,
      record.total_sulfur_dioxide.toFloat,
      record.density.toFloat,
      record.pH.toFloat,
      record.sulphates.toFloat,
      record.alcohol.toFloat)
    Tensor.create(Array(data))
  }

  val modelName = "WineTensorFlowModel"

  def make(input: ModelToServe): Model[WineRecord, Double] =
    new WineTensorFlowModel(input.model)

  def make(bytes: Array[Byte]): Model[WineRecord, Double] =
    new WineTensorFlowModel(bytes)
}
