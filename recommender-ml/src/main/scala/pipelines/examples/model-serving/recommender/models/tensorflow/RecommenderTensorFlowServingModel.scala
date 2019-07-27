package pipelines.examples.modelserving.recommender.models.tensorflow

import pipelines.examples.modelserving.recommender.data.{ ModelResult, ModelResultMetadata, ProductPrediction, RecommenderRecord, RecommenderResult }
import com.lightbend.modelserving.model.{ Model, ModelFactory, ModelDescriptor, ModelDescriptorUtil, ModelType, ScoreMetadata }
import com.lightbend.modelserving.model.tensorflow.TensorFlowServingModel
import com.google.gson.Gson

class RecommenderTensorFlowServingModel(descriptor: ModelDescriptor)
  extends TensorFlowServingModel[RecommenderRecord, RecommenderResult, TFRequest, TFPredictionResult](descriptor) {

  override val clazz: Class[TFPredictionResult] = classOf[TFPredictionResult]

  override def getHTTPRequest(input: RecommenderRecord): TFRequest = {
    val products = input.products.map(Array(_)).toArray
    val users = input.products.map(_ ⇒ Array(input.user)).toArray
    TFRequest("", TFRequestInputs(products, users))
  }

  protected def initFrom(record: RecommenderRecord): RecommenderResult =
    new RecommenderResult(
      modelResult = new ModelResult( // will be overwritten subsequently
        predictions = Nil),
      modelResultMetadata = new ModelResultMetadata( // will be overwritten subsequently
        errors = "",
        modelType = ModelType.TENSORFLOWSERVING.ordinal,
        modelName = "RecommenderTensorFlowServingModel",
        duration = 0),
      user = record.user,
      products = record.products)

  protected def setScoreAndMetadata(
      out:      RecommenderResult,
      score:    Option[TFPredictionResult],
      metadata: ScoreMetadata): RecommenderResult = {
    val predictions: Seq[ProductPrediction] = score match {
      case None ⇒ Nil
      case Some(tfpr) ⇒
        val recoms = tfpr.outputs.recommendations.map(_(0)) // take first recommendation result in each nested array
        out.products.zip(recoms).map(r ⇒ ProductPrediction(r._1, r._2))
    }
    out.modelResult.predictions = predictions
    out.modelResultMetadata.errors = metadata.errors
    out.modelResultMetadata.modelType = metadata.modelType.ordinal
    out.modelResultMetadata.modelName = metadata.modelName
    out.modelResultMetadata.duration = metadata.duration.length
    out
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
object RecommenderTensorFlowServingModelFactory extends ModelFactory[RecommenderRecord, RecommenderResult] {

  /**
   * Creates a new TensorFlow serving model.
   *
   * @param descriptor model to serve representation of TensorFlow serving model.
   * @return model
   */
  def make(
      descriptor: ModelDescriptor): Either[String, Model[RecommenderRecord, RecommenderResult]] =
    if (descriptor == Model.noopModelDescriptor) Right(noopModel)
    else Right(new RecommenderTensorFlowServingModel(descriptor))

  lazy val noopModel: Model[RecommenderRecord, RecommenderResult] =
    new RecommenderTensorFlowServingModel(Model.noopModelDescriptor) with Model.NoopModel[RecommenderRecord, TFPredictionResult, RecommenderResult] {

      override protected def init(): String = "no bytes"
      override protected def invokeModel(record: RecommenderRecord): (String, Option[TFPredictionResult]) =
        noopInvokeModel(record)
    }
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

