package pipelines.examples.modelserving.recommedermodel.tensorflow

import com.lightbend.modelserving.model.{Model, ModelFactory, ModelToServe}
import pipelines.examples.data.{ProductPrediction, RecommenderRecord}
import com.lightbend.modelserving.model.tensorflow.TensorFlowServingModel

class RecommenderTensorflowServingModel(inputStream: Array[Byte]) extends
  TensorFlowServingModel[RecommenderRecord, Seq[ProductPrediction], TFRequest, TFPredictionResult](inputStream){

  override val clazz: Class[TFPredictionResult] = classOf[TFPredictionResult]

  override def getHTTPRequest(input: RecommenderRecord): TFRequest = {
    val products = input.products.map(Array(_)).toArray
    val users = input.products.map(_ => Array(input.user)).toArray
    TFRequest("", TFRequestInputs(products, users))
  }

  override def getResult(result: TFPredictionResult, input: RecommenderRecord): Seq[ProductPrediction] = {
    result.outputs.recommendations.map(_ (0))
      .zip (input.products) map (r => ProductPrediction(r._2, r._1))
  }

}

/**
  * Implementation of TensorFlow serving model factory.
  */
object RecommenderTensorflowServingModel extends ModelFactory[RecommenderRecord, Seq[ProductPrediction]] {

  /**
    * Creates a new TensorFlow serving model.
    *
    * @param descriptor model to serve representation of TensorFlow serving model.
    * @return model
    */
  override def create(input: ModelToServe): Option[Model[RecommenderRecord, Seq[ProductPrediction]]] =
    try
      Some(new RecommenderTensorflowServingModel(input.location.getBytes))
    catch {
      case _: Throwable => None
    }

  /**
    * Restore model from binary.
    *
    * @param bytes binary representation of TensorFlow serving model.
    * @return model
    */
  override def restore(bytes: Array[Byte]): Model[RecommenderRecord, Seq[ProductPrediction]] =
    new RecommenderTensorflowServingModel(bytes)
}

