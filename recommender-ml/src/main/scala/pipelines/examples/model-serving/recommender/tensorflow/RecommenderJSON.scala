package pipelines.examples.modelserving.recommendermodel.tensorflow

import com.google.gson.annotations.SerializedName

// Case classes for json mapping
case class TFRequestInputs(products: Array[Array[Long]], users: Array[Array[Long]])
case class TFRequest(signature_name: String, inputs: TFRequestInputs)
case class RecommendationOutputs(@SerializedName("model-version") model: Array[Int], recommendations: Array[Array[Double]])
case class TFPredictionResult(outputs: RecommendationOutputs)
