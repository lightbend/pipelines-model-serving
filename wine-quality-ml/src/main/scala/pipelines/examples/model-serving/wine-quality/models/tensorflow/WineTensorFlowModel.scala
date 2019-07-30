package pipelines.examples.modelserving.winequality.models.tensorflow

import com.lightbend.modelserving.model.tensorflow.TensorFlowModel
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelFactory }
import pipelines.examples.modelserving.winequality.WineModelCommon
import pipelines.examples.modelserving.winequality.data.{ WineRecord, WineResult }
import org.tensorflow.Tensor
import org.tensorflow.{ Graph, Session }

/**
 * TensorFlow model implementation for wine data
 */
class WineTensorFlowModel(metadata: ModelDescriptor)
  extends TensorFlowModel[WineRecord, Double, WineResult](metadata)
  with WineModelCommon {

  import WineTensorFlowModel._

  /** Score a wine record with the model */
  override protected def invokeModel(record: WineRecord): (String, Option[Double]) = {
    // Create modelInput tensor
    val modelInput = toTensor(record)
    // Serve model using TensorFlow APIs
    val result = session.runner.feed("dense_1_input", modelInput).fetch("dense_3/Sigmoid").run().get(0)
    // Get result shape
    val rshape = result.shape
    // Map output tensor to shape
    val rMatrix = Array.ofDim[Float](rshape(0).asInstanceOf[Int], rshape(1).asInstanceOf[Int])
    result.copyTo(rMatrix)
    // Get result
    ("", Some(rMatrix(0).indices.maxBy(rMatrix(0)).toDouble))
  }
}

object WineTensorFlowModel {

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
}

/** Factory for wine data PMML model */
object WineTensorFlowModelFactory extends ModelFactory[WineRecord, WineResult] {

  def make(descriptor: ModelDescriptor): Either[String, Model[WineRecord, WineResult]] =
    Right(new WineTensorFlowModel(descriptor))
}
