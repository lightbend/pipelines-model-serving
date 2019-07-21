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

package com.lightbend.modelserving.model

import java.io.DataOutputStream

/**
 * Used to construct new models, persist existing ones, and reconstitute persisted models.
 */
final class ModelManager[RECORD, RESULT](resolver: ModelFactoryResolver[RECORD, RESULT]) {

  /** Get the model from byte array */
  def fromModelRecord(message: ModelDescriptor): ModelToServe = {
    message.modeldata match {
      case Some(data) => new ModelToServe(message.name, message.description, message.modeltype.ordinal, data, null, message.dataType)
      case _ => new ModelToServe(message.name, message.description, message.modeltype.ordinal, Array[Byte](), message.modeldatalocation.get, message.dataType)
    }
  }

  override def toString: String = super.toString

  /** Write the model to data stream */
  def writeModel(model: Model[RECORD, RESULT], output: DataOutputStream): Unit = {
    try {
      if (model == null) {
        output.writeLong(0)
        return
      }
      val bytes = model.toBytes()
      output.writeLong(bytes.length)
      output.writeLong(model.getType.ordinal)
      output.write(bytes)
    } catch {
      case t: Throwable =>
        System.out.println("Error Serializing model")
        t.printStackTrace()
    }
  }

  final case class CopyError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
  final case object ModelResolverMissing extends RuntimeException("Model factory resolver is not set")

  /**
   * Deep copy the model
   * TODO: This should be a method on the Model case class itself.
   */
  def copy(from: Option[Model[RECORD, RESULT]]): Option[Model[RECORD, RESULT]] =
    from.map { model =>
      validateResolver()
      resolver.getFactory(model.getType.ordinal) match {
        case None =>
          throw CopyError(s"BUG: ModelToServe.copy could not find a corresponding model factory for model: $model.")
        case Some(factory) =>
          factory.restore(model.toBytes()) match {
            case Right(model) => model
            case Left(error) =>
              throw CopyError(s"ModelToServe.copy failed to copy model: $model. $error")
          }
      }
    }

  /** Restore model of the specified ModelType from a byte array */
  def restore(t: ModelType, content: Array[Byte]): Either[String, Model[RECORD, RESULT]] =
    restore(t.ordinal(), content)

  /** Restore model of the specified ModelType value from a byte array */
  def restore(ordinal: Int, content: Array[Byte]): Either[String, Model[RECORD, RESULT]] = try {
    validateResolver()
    val factory = resolver.getFactory(ordinal).get
    factory.restore(content)
  } catch {
    case scala.util.control.NonFatal(th) =>
      Left(s"ModelToServe.restore(ordinal = $ordinal): Failed to restore the model. $th")
  }

  /** Get the model from ModelToServe */
  def toModel(model: ModelToServe): Either[String, Model[RECORD, RESULT]] = {
    def errPrefix = s"ModelToServe.toModel(model = $model): "
    try {
      validateResolver()
      resolver.getFactory(model.modelType) match {
        case Some(factory) => factory.create(model) match {
          case Right(model) => Right(model)
          case Left(error) => Left(s"$errPrefix Factory found, but it could not create the model. $error")
        }
        case _ => Left(s"$errPrefix No factory found for model type ${model.modelType}.")
      }
    } catch {
      case scala.util.control.NonFatal(th) =>
        Left(s"ModelToServe.toModel(model = $model) failed: $th. ${formatStackTrace(th)}")
    }
  }

  private def formatStackTrace(th: Throwable): String = th.getStackTrace().mkString("\n  ", "\n  ", "\n")

  /** Ensure that the resolver is set */
  private def validateResolver(): Unit =
    if (resolver == null) throw ModelResolverMissing
}

