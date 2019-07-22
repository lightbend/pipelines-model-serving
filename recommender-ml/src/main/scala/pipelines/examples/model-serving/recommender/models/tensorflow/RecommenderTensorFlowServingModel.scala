package pipelines.examples.modelserving.recommender.models.tensorflow

import pipelines.examples.modelserving.recommender.data.{ ProductPrediction, RecommenderRecord }
import com.lightbend.modelserving.model.{ Model, ModelFactory, ModelMetadata }
import com.lightbend.modelserving.model.tensorflow.TensorFlowServingModel
import com.google.gson.Gson

class RecommenderTensorFlowServingModel(metadata: ModelMetadata)
  extends TensorFlowServingModel[RecommenderRecord, Seq[ProductPrediction], TFRequest, TFPredictionResult](metadata) {

  override val clazz: Class[TFPredictionResult] = classOf[TFPredictionResult]

  override def getHTTPRequest(input: RecommenderRecord): TFRequest = {
    val products = input.products.map(Array(_)).toArray
    val users = input.products.map(_ ⇒ Array(input.user)).toArray
    TFRequest("", TFRequestInputs(products, users))
  }

  override def getResult(
    result: TFPredictionResult,
    input: RecommenderRecord): Either[String, Seq[ProductPrediction]] = {
    // is it possible we'll get error results??
    val predictions = result.outputs.recommendations.map(_(0))
      .zip(input.products).map(r ⇒ ProductPrediction(r._2, r._1))
    Right(predictions)
  }

  // Test method to ensure that transformation works correctly
  def transformer(input: RecommenderRecord, servingResult: String): Unit = {

    val hTTPRec = gson.toJson(getHTTPRequest(input))
    println(s" input to json : $hTTPRec")
    val res = gson.fromJson(servingResult, clazz)
    val result = getResult(res, input)
    println(s"execution result $result")
  }
}

/**
 * Implementation of TensorFlow serving model factory.
 */
object RecommenderTensorFlowServingModel extends ModelFactory[RecommenderRecord, Seq[ProductPrediction]] {

  val modelName = "RecommenderTensorFlowServingModel"

  /**
   * Creates a new TensorFlow serving model.
   *
   * @param descriptor model to serve representation of TensorFlow serving model.
   * @return model
   */
  def make(metadata: ModelMetadata): Model[RecommenderRecord, Seq[ProductPrediction]] =
    new RecommenderTensorFlowServingModel(metadata)

  // Testing transformation
  def main(args: Array[String]): Unit = {

    val gson = new Gson

    val metadata = ModelMetadata.unknown
    val model = new RecommenderTensorFlowServingModel(metadata)
    val record = new RecommenderRecord(10L, Seq(1L, 2L, 3L, 4L), "recommender")
    val httpRes = gson.toJson(new TFPredictionResult(new RecommendationOutputs(
      Seq(1, 2, 3).toArray,
      Seq(Seq(.1).toArray, Seq(.2).toArray, Seq(.3).toArray, Seq(.4).toArray).toArray)))
    model.transformer(record, httpRes)
  }
}

