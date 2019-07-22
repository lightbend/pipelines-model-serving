package com.lightbend.modelserving.model.tensorflow

import com.lightbend.modelserving.model.{ Model, ModelMetadata, ModelType }

import com.google.gson.Gson
import scalaj.http.Http

/**
 * Abstract class for any TensorFlow serving model processing. It has to be extended by the user to
 * implement supporting methods (data transforms), based on his own model. Serializability here is required for Spark.
 */

abstract class TensorFlowServingModel[RECORD, RESULT, HTTPREQUEST, HTTPRESULT](
  val metadata: ModelMetadata)
  extends Model[RECORD, RESULT] with Serializable {

  val gson = new Gson
  val clazz: Class[HTTPRESULT]

  // Make sure data is not empty
  if (metadata.modelBytes.length < 1) throw new Exception("Empty URL")
  // Convert input into file path
  var path = new String(metadata.modelBytes)

  // Nothing to cleanup in this case
  override def cleanup(): Unit = {}

  /** Convert incoming request to HTTP */
  def getHTTPRequest(input: RECORD): HTTPREQUEST

  /** Convert HTTPResult to Result */
  def getResult(result: HTTPRESULT, input: RECORD): Either[String, RESULT]

  /** Score a record with the model */
  override def score(input: RECORD): Either[String, RESULT] = {
    // Post request
    val result = Http(path).postData(gson.toJson(getHTTPRequest(input))).header("content-type", "application/json").asString
    result.code match {
      case 200 => // Success
        val prediction = gson.fromJson(result.body, clazz)
        getResult(prediction, input)
      case _ => // Error
        Left(s"Error processing serving request - code ${result.code}, error ${result.body}")
    }
  }
}
