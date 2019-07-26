package pipelines.examples.modelserving.recommender.models.tensorflow

import pipelines.examples.modelserving.recommender.data.{ ProductPrediction, RecommenderRecord }
import com.lightbend.modelserving.model.{ Model, ModelFactory, ModelDescriptor, ModelDescriptorUtil, ModelType }
import com.lightbend.modelserving.model.tensorflow.TensorFlowServingModel
import com.google.gson.Gson

class RecommenderTensorFlowServingModel(descriptor: ModelDescriptor)
  extends TensorFlowServingModel[RecommenderRecord, Seq[ProductPrediction], TFRequest, TFPredictionResult](descriptor) {

  override val clazz: Class[TFPredictionResult] = classOf[TFPredictionResult]

  override def getHTTPRequest(input: RecommenderRecord): TFRequest = {
    val products = input.products.map(Array(_)).toArray
    val users = input.products.map(_ ⇒ Array(input.user)).toArray
    TFRequest("", TFRequestInputs(products, users))
  }

  protected def makeOutRecord(
      record:    RecommenderRecord,
      errors:    String,
      score:     TFPredictionResult,
      duration:  Long,
      modelName: String,
      modelType: ModelType): Seq[ProductPrediction] = {
    // The basic Seq[] return type means we can't return metadata, including errors (TODO)
    val predictions = score.outputs.recommendations.map(_(0))
      .zip(record.products).map(r ⇒ ProductPrediction(r._2, r._1))
    predictions
  }

  // Test method to ensure that transformation works correctly
  // TODO: replace this method and main below with a call through the full scoring
  // pipeline.
  def transformer(record: RecommenderRecord, servingResult: String): Unit = {
    val hTTPRec = gson.toJson(getHTTPRequest(record))
    println(s" record to json : $hTTPRec")
    val res = gson.fromJson(servingResult, clazz)
    val result = res.outputs.recommendations.map(_(0))
      .zip(record.products).map(r ⇒ ProductPrediction(r._2, r._1))
    println(s"execution result $result")
  }
}

/**
 * Implementation of TensorFlow serving model factory.
 */
object RecommenderTensorFlowServingModelFactory extends ModelFactory[RecommenderRecord, Seq[ProductPrediction]] {

  /**
   * Creates a new TensorFlow serving model.
   *
   * @param descriptor model to serve representation of TensorFlow serving model.
   * @return model
   */
  def make(descriptor: ModelDescriptor): Either[String, Model[RecommenderRecord, Seq[ProductPrediction]]] =
    Right(new RecommenderTensorFlowServingModel(descriptor))
}

/**
 * Test program for [[RecommenderModelServer]]. Just loads the TensorFlow Serving
 * model and uses it to score one record. So, this program focuses on ensuring
 * the logic works for any model, but doesn't exercise all the available models.
 * For testing purposes, only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.recommender.models.tensorflow._
 * RecommenderTensorFlowServingModelMain.main(Array())
 * ```
 */
object RecommenderTensorFlowServingModelMain {
  // Testing transformation
  def main(args: Array[String]): Unit = {
    val descriptor = ModelDescriptorUtil.unknown.copy(
      modelBytes =
        Some("http://recommender-service-kubeflow.foobarserver.lightbend.com/v1/models/recommender/versions/1:predict".getBytes()))
    val model = new RecommenderTensorFlowServingModel(descriptor)
    val record = new RecommenderRecord(10L, Seq(1L, 2L, 3L, 4L))
    val gson = new Gson
    val httpRes = gson.toJson(new TFPredictionResult(new RecommendationOutputs(
      Seq(1, 2, 3).toArray,
      Seq(Seq(.1).toArray, Seq(.2).toArray, Seq(.3).toArray, Seq(.4).toArray).toArray)))
    val result = model.transformer(record, httpRes)
    println(s"result: $result")
  }
}

