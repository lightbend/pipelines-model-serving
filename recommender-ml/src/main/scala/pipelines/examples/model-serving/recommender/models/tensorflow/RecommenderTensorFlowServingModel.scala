package pipelines.examples.modelserving.recommender.models.tensorflow

import pipelines.examples.modelserving.recommender.data.RecommenderRecord
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelDescriptorUtil, ModelFactory }
import com.lightbend.modelserving.model.tensorflow.TensorFlowServingModel
import com.google.gson.Gson
import pipelines.examples.modelserving.recommender.result.ModelKeyDoubleValueArrayResult

class RecommenderTensorFlowServingModel(descriptor: ModelDescriptor)
  extends TensorFlowServingModel[RecommenderRecord, TFRequest, TFPredictionResult](
    descriptor)(() ⇒ RecommenderTensorFlowServingModel.makeEmptyTFPredictionResult()) {

  override val clazz: Class[TFPredictionResult] = classOf[TFPredictionResult]

  override def getHTTPRequest(input: RecommenderRecord): TFRequest = {
    val products = input.products.map(Array(_)).toArray
    val users = input.products.map(_ ⇒ Array(input.user)).toArray
    TFRequest("", TFRequestInputs(products, users))
  }

  // Test method to ensure that transformation works correctly
  // TODO: replace this method and main below with a call through the full scoring
  // pipeline.
  def transformer(record: RecommenderRecord, servingResult: String): Unit = {
    val hTTPRec = gson.toJson(getHTTPRequest(record))
    println(s" record to json : $hTTPRec")
    val res = gson.fromJson(servingResult, clazz)
    val (keys, values) =
      RecommenderTensorFlowServingModel.predictionToKeyValueArray(record, res)
    val result = ModelKeyDoubleValueArrayResult(keys = keys, values = values)
    println(s"execution result $result")
  }
}

object RecommenderTensorFlowServingModel {

  def makeEmptyTFPredictionResult() =
    TFPredictionResult(
      outputs = RecommendationOutputs(
        model = Array.empty[Int],
        recommendations = Array.empty[Array[Double]]))

  def predictionToKeyValueArray(record: RecommenderRecord, tfpr: TFPredictionResult): (Array[String], Array[Double]) = {
    tfpr.outputs.recommendations.map(_(0)) // take first element in each subarray
      .zip(record.products).map(r ⇒ (r._2.toString, r._1)).unzip
  }
}

/**
 * Implementation of TensorFlow serving model factory.
 */
object RecommenderTensorFlowServingModelFactory extends ModelFactory[RecommenderRecord, TFPredictionResult] {

  /**
   * Creates a new TensorFlow serving model.
   *
   * @param descriptor model to serve representation of TensorFlow serving model.
   * @return model
   */
  def make(
      descriptor: ModelDescriptor): Either[String, Model[RecommenderRecord, TFPredictionResult]] =
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
      Array(1, 2, 3),
      Array(Array(.1), Array(.2), Array(.3), Array(.4)))))
    val result = model.transformer(record, httpRes)
    println(s"result: $result")
  }
}

