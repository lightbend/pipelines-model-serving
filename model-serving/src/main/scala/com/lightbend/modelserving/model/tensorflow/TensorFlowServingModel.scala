package com.lightbend.modelserving.model.tensorflow

import com.lightbend.modelserving.model.{ ModelBase, ModelDescriptor }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

import com.google.gson.Gson
import scalaj.http.Http // TODO: Replace with a Lightbend library??

/**
 * Abstract class for any TensorFlow serving model processing. It has to be extended by the user to
 * implement supporting methods (data transforms), based on his own model. Serializability here is required for Spark.
 */

abstract class TensorFlowServingModel[INRECORD, HTTPREQUEST, HTTPRESULT](
    descriptor: ModelDescriptor)(makeDefaultModelOutput: () ⇒ HTTPRESULT)
  extends ModelBase[INRECORD, HTTPRESULT](descriptor)(makeDefaultModelOutput) with Serializable {

  assert(descriptor.modelSourceLocation != None, s"Invalid descriptor ${descriptor.toRichString}")

  val gson = new Gson
  val clazz: Class[HTTPRESULT]

  // Convert input into file path
  val path = descriptor.modelSourceLocation.get

  /** Convert incoming request to HTTP */
  def getHTTPRequest(input: INRECORD): HTTPREQUEST

  /** Score a record with the model */
  override protected def invokeModel(input: INRECORD): Either[String, HTTPRESULT] = {
    // Post request
    try {
      val result = Http(path).postData(gson.toJson(getHTTPRequest(input))).header("content-type", "application/json").asString
      val prediction = gson.fromJson(result.body, clazz)
      result.code match {
        case 200 ⇒ // Success
          Right(prediction)
        case _ ⇒ // Error
          Left(s"Error processing serving request - code ${result.code}, error ${result.body}")
      }
    }catch {
      case t:Throwable =>
        Left(t.getMessage)
    }
  }
}
