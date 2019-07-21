package pipelines.examples.modelserving.winequality.models.tensorflow

import com.lightbend.modelserving.model.tensorflow.TensorFlowBundleModel
import com.lightbend.modelserving.model.{ Model, ModelFactory }
import com.lightbend.modelserving.model.ModelToServe
import pipelines.examples.modelserving.winequality.data.WineRecord

/**
 * Implementation of TensorFlow bundled model for Wine.
 */
class WineTensorFlowBundledModel(inputStream: Array[Byte]) extends TensorFlowBundleModel[WineRecord, Double](inputStream) {

  override def score(input: WineRecord): Either[String, Double] = try {
    // Create input tensor
    val modelInput = WineTensorFlowModel.toTensor(input)
    // Serve model using TensorFlow APIs
    val signature = signatures.head._2
    val tinput = signature.inputs.head._2
    val toutput = signature.outputs.head._2
    val result = session.runner.feed(tinput.name, modelInput).fetch(toutput.name).run().get(0)
    // process result
    val rshape = result.shape
    val rMatrix = Array.ofDim[Float](rshape(0).asInstanceOf[Int], rshape(1).asInstanceOf[Int])
    result.copyTo(rMatrix)
    Right(rMatrix(0).indices.maxBy(rMatrix(0)).toDouble)
  } catch {
    case scala.util.control.NonFatal(th) =>
      Left("WineTensorFlowBundledModel.score failed: $th")
  }
}

/**
 * Implementation of TensorFlow bundled model factory.
 */
object WineTensorFlowBundledModel extends ModelFactory[WineRecord, Double] {

  val modelName = "WineTensorFlowBundledModel"

  /**
   * Creates a new TensorFlow bundled model.
   *
   * @param descriptor model to serve representation of TensorFlow bundled model.
   * @return model
   */
  def make(input: ModelToServe): Model[WineRecord, Double] =
    new WineTensorFlowBundledModel(input.location.getBytes)

  /**
   * Restore PMML model from binary.
   *
   * @param bytes binary representation of TensorFlow bundled model.
   * @return model
   */
  def make(bytes: Array[Byte]): Model[WineRecord, Double] =
    new WineTensorFlowBundledModel(bytes)
}

