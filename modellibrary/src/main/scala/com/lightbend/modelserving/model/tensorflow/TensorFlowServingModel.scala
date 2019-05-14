/*
 * Copyright (C) 2017-2019  Lightbend
 *
 * This file is part of the Lightbend model-serving-tutorial (https://github.com/lightbend/model-serving-tutorial)
 *
 * The model-serving-tutorial is free software: you can redistribute it and/or modify
 * it under the terms of the Apache License Version 2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lightbend.modelserving.model.tensorflow

import java.io.{ObjectInputStream, ObjectOutputStream}

import akka.actor.ActorSystem

import scala.util.{Failure, Success}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.lightbend.modelserving.model.Model
import pipelines.examples.data.ModelType
import scala.reflect._
import com.google.gson.Gson

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * Abstract class for any TensorFlow serving model processing. It has to be extended by the user to
 * implement supporting methods (data transforms), based on his own model. Serializability here is required for Spark.
 */
abstract class TensorFlowServingModel[RECORD, RESULT, HTTPREQUEST, HTTPRESULT](inputStream: Array[Byte])
  (implicit system : ActorSystem, materializer : ActorMaterializer, executionContext : ExecutionContextExecutor)
  extends Model[RECORD, RESULT] with Serializable {

  val gson = new Gson
  implicit val tag: ClassTag[HTTPRESULT]

  // Make sure data is not empty
  if (inputStream.length < 1) throw new Exception("Empty URL")
  // Convert input into file path
  var path = new String(inputStream)

  // Nothing to cleanup in this case
  override def cleanup(): Unit = {}

  /** Convert the TensorFlow model to bytes */
  override def toBytes(): Array[Byte] = inputStream

  /** Get model type */
  override def getType = ModelType.TENSORFLOWSERVING

  override def equals(obj: Any): Boolean = {
    obj match {
      case tfModel : TensorFlowServingModel[RECORD, RESULT, HTTPREQUEST, HTTPRESULT] =>
        tfModel.toBytes.toList == inputStream.toList
      case _ => false
    }
  }

  // Convert incoming request to HTTP
  def getHTTPRequest(input : RECORD) : HTTPREQUEST

  // Convert HTTPResult to Result
  def getResult(result : HTTPRESULT) : RESULT

  /** Score a record with the model */
  override def score(input: RECORD) : RESULT = {

    // Post request
    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(
      method = HttpMethods.POST,
      uri = path,
      entity = HttpEntity(ContentTypes.`application/json`, gson.toJson(getHTTPRequest(input)))
    ))

    // Get Result
    var result = null.asInstanceOf[RESULT]
    responseFuture
      .onComplete {
        case Success(res) => {
          Unmarshal(res.entity).to[String].map(string => {
            val prediction = gson.fromJson(string, tag.runtimeClass).asInstanceOf[HTTPRESULT]
            result = getResult(prediction)
          })}
        case Failure(t) => {
          sys.error(s"something wrong $t")
        }
      }
    result
  }

  private def writeObject(output: ObjectOutputStream): Unit = {
    val start = System.currentTimeMillis()
    output.writeObject(inputStream)
    println(s"TensorFlow serialization in ${System.currentTimeMillis() - start} ms")
  }

  private def readObject(input: ObjectInputStream): Unit = {
    val start = System.currentTimeMillis()
    val bytes = input.readObject().asInstanceOf[Array[Byte]]
    try {
      path = new String(bytes)
      println(s"TensorFlow serving deserialization in ${System.currentTimeMillis() - start} ms")
    } catch {
      case t: Throwable =>
        t.printStackTrace
        println(s"TensorFlow serving deserialization failed in ${System.currentTimeMillis() - start} ms")
        println(s"Restored TensorFlow ${new String(bytes)}")
    }
  }
}
