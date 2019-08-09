package pipelines.examples.modelserving.recommender.models.tensorflow

import pipelines.examples.modelserving.recommender.data.RecommenderRecord
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelFactory }
import com.lightbend.modelserving.model.tensorflow.TensorFlowServingModel
import pipelines.examples.modelserving.recommender.result.ModelKeyDoubleValueArrayResult

class RecommenderTensorFlowServingModel(descriptor: ModelDescriptor)
  extends TensorFlowServingModel[RecommenderRecord, ModelKeyDoubleValueArrayResult, TFRequest, TFPredictionResult](
    descriptor)(() ⇒ RecommenderTensorFlowServingModel.makeEmptyTFPredictionResult()) {

  override val clazz: Class[TFPredictionResult] = classOf[TFPredictionResult]

  override def getHTTPRequest(input: RecommenderRecord): TFRequest = {
    val products = input.products.map(Array(_)).toArray
    val users = input.products.map(_ ⇒ Array(input.user)).toArray
    TFRequest("", TFRequestInputs(products, users))
  }

  /** Convert HTTP Result to model output */
  override def getModelOutput(record: RecommenderRecord, res: TFPredictionResult): ModelKeyDoubleValueArrayResult = {
    val (keys, values) =
      RecommenderTensorFlowServingModel.predictionToKeyValueArray(record, res)
    ModelKeyDoubleValueArrayResult(keys = keys, values = values)
  }
}

object RecommenderTensorFlowServingModel {

  def makeEmptyTFPredictionResult() =
    ModelKeyDoubleValueArrayResult(Seq.empty, Seq.empty)

  def predictionToKeyValueArray(record: RecommenderRecord, tfpr: TFPredictionResult): (Array[String], Array[Double]) = {
    tfpr.outputs.recommendations.map(_(0)) // take first element in each subarray
      .zip(record.products).map(r ⇒ (r._2.toString, r._1)).unzip
  }
}

/**
 * Implementation of TensorFlow serving model factory.
 */
object RecommenderTensorFlowServingModelFactory extends ModelFactory[RecommenderRecord, ModelKeyDoubleValueArrayResult] {

  /**
   * Creates a new TensorFlow serving model.
   *
   * @param descriptor model to serve representation of TensorFlow serving model.
   * @return model
   */
  def make(
      descriptor: ModelDescriptor): Either[String, Model[RecommenderRecord, ModelKeyDoubleValueArrayResult]] =
    Right(new RecommenderTensorFlowServingModel(descriptor))
}
