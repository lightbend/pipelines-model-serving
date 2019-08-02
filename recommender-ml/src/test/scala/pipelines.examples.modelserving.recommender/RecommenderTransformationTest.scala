package pipelines.examples.modelserving.recommender

import com.google.gson.Gson
import com.lightbend.modelserving.model.{ModelDescriptor, ModelType}
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.recommender.data.RecommenderRecord
import pipelines.examples.modelserving.recommender.models.tensorflow.{RecommendationOutputs, RecommenderTensorFlowServingModel, RecommenderTensorFlowServingModelFactory, TFPredictionResult}
import pipelines.examples.modelserving.recommender.result.ModelKeyDoubleValueArrayResult

class RecommenderTransformationTest extends FlatSpec {

  val gson = new Gson()
  val clazz: Class[TFPredictionResult] = classOf[TFPredictionResult]

  private def getModel(): ModelDescriptor = {
    new ModelDescriptor(
      modelName = "Recommender model",
      description = "Recommender tensorflow serving model",
      modelType = ModelType.TENSORFLOWSERVING,
      modelBytes = None,
      modelSourceLocation = Some("http://recommender-service-kubeflow.foobarserver.lightbend.com/v1/models/recommender/versions/1:predict"))
  }

  // Data transformer
  def transformer(model : RecommenderTensorFlowServingModel,record: RecommenderRecord, servingResult: String): Unit = {
    val hTTPRec = gson.toJson(model.getHTTPRequest(record))
    println(s" record to json : $hTTPRec")
    val res = gson.fromJson(servingResult, clazz)
    val (keys, values) =
      RecommenderTensorFlowServingModel.predictionToKeyValueArray(record, res)
    val result = ModelKeyDoubleValueArrayResult(keys = keys, values = values)
    println(s"execution result $result")
  }

  "A recommender " should "provide a proper result" in {
    val record = new RecommenderRecord(10L, Seq(1L, 2L, 3L, 4L))
    val httpRes = gson.toJson(new TFPredictionResult(new RecommendationOutputs(
      Array(1, 2, 3),
      Array(Array(.1), Array(.2), Array(.3), Array(.4)))))
    val descriptor = getModel()
    RecommenderTensorFlowServingModelFactory.create(descriptor) match {
      case Right(model) =>
        val result = transformer(model.asInstanceOf[RecommenderTensorFlowServingModel], record, httpRes)
        println(s"result: $result")
      case Left(reason) =>
        throw new Exception(reason)
    }
  }
}