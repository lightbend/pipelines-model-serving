package pipelines.examples.modelserving.winequality.models.tensorflow

import com.lightbend.modelserving.model.tensorflow.TensorFlowBundleModel
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelFactory }
import com.lightbend.modelserving.model.ModelDescriptor
import pipelines.examples.modelserving.winequality.data.WineRecord

/**
 * Implementation of TensorFlow bundled model for Wine.
 */
class WineTensorFlowBundledModel(descriptor: ModelDescriptor)
  extends TensorFlowBundleModel[WineRecord, Double](descriptor)(() ⇒ 0.0) {

  override protected def invokeModel(record: WineRecord): Either[String, Double] = {
    try {
      // Create record tensor
      val modelInput = WineTensorFlowModel.toTensor(record)
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
      case t: Throwable ⇒ Left(t.getMessage)
    }
  }
}

/**
 * Implementation of TensorFlow bundled model factory.
 */
object WineTensorFlowBundledModelFactory extends ModelFactory[WineRecord, Double] {

  def make(descriptor: ModelDescriptor): Either[String, Model[WineRecord, Double]] =
    Right(new WineTensorFlowBundledModel(descriptor))
}

