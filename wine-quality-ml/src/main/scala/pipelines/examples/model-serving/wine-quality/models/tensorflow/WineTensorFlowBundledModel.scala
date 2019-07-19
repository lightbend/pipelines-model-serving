package pipelines.examples.modelserving.winemodel.tensorflow

import com.lightbend.modelserving.model.tensorflow.TensorFlowBundleModel
import com.lightbend.modelserving.model.{ Model, ModelFactory }
import com.lightbend.modelserving.model.ModelToServe
import pipelines.examples.data.WineRecord

/**
 * Implementation of TensorFlow bundled model for Wine.
 */
class WineTensorFlowBundledModel(inputStream: Array[Byte]) extends TensorFlowBundleModel[WineRecord, Double](inputStream) {

  /**
   * Score data.
   *
   * @param input object to score.
   * @return scoring result
   */
  override def score(input: WineRecord): Double = {
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
    rMatrix(0).indices.maxBy(rMatrix(0)).toDouble
  }
}

/**
 * Implementation of TensorFlow bundled model factory.
 */
object WineTensorFlowBundledModel extends ModelFactory[WineRecord, Double] {

  /**
   * Creates a new TensorFlow bundled model.
   *
   * @param descriptor model to serve representation of TensorFlow bundled model.
   * @return model
   */
  override def create(input: ModelToServe): Option[Model[WineRecord, Double]] =
    try
      Some(new WineTensorFlowBundledModel(input.location.getBytes))
    catch {
      case _: Throwable ⇒ None
    }

  /**
   * Restore PMML model from binary.
   *
   * @param bytes binary representation of TensorFlow bundled model.
   * @return model
   */
  override def restore(bytes: Array[Byte]): Model[WineRecord, Double] = new WineTensorFlowBundledModel(bytes)
}

