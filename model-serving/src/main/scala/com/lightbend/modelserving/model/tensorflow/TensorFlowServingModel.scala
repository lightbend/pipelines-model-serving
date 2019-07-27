package com.lightbend.modelserving.model.tensorflow

import com.lightbend.modelserving.model.{ ModelBase, ModelDescriptor }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

import com.google.gson.Gson
import scalaj.http.Http // TODO: Replace with a Lightbend library??

/**
 * Abstract class for any TensorFlow serving model processing. It has to be extended by the user to
 * implement supporting methods (data transforms), based on his own model. Serializability here is required for Spark.
 */

abstract class TensorFlowServingModel[INRECORD, OUTRECORD, HTTPREQUEST, HTTPRESULT](
    descriptor: ModelDescriptor)
  extends ModelBase[INRECORD, HTTPRESULT, OUTRECORD](descriptor) with Serializable {

  assert(descriptor.modelBytes != None, s"Invalid descriptor ${descriptor.toRichString}")

  val gson = new Gson
  val clazz: Class[HTTPRESULT]

  // Convert input into file path
  // TODO: Put this in the 'modelSourceLocation' field instead!
  protected def init(): String = new String(descriptor.modelBytes.get)

  var path = init()

  // Nothing to cleanup in this case
  override def cleanup(): Unit = {}

  /** Convert incoming request to HTTP */
  def getHTTPRequest(input: INRECORD): HTTPREQUEST

  /** Score a record with the model */
  override protected def invokeModel(input: INRECORD): (String, Option[HTTPRESULT]) = {
    // Post request
    val result = Http(path).postData(gson.toJson(getHTTPRequest(input))).header("content-type", "application/json").asString
    val prediction = Some(gson.fromJson(result.body, clazz))
    result.code match {
      case 200 ⇒ // Success
        ("", prediction)
      case _ ⇒ // Error
        (s"Error processing serving request - code ${result.code}, error ${result.body}",
          prediction)
    }
  }
}
