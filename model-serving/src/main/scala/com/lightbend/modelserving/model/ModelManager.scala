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

  override def toString: String = super.toString

  final case class CopyError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

  /**
   * Deep copy the model
   * TODO: This should be a method on the Model case class itself.
   * @deprecated
   */
  def copy(from: Option[Model[RECORD, RESULT]]): Option[Model[RECORD, RESULT]] =
    from.map { model =>
      create(model.metadata) match {
        case Left(error) =>
          throw CopyError(s"BUG: ModelMetadata.copy failed for model $model. $error")
        case Right(modelCopy) => modelCopy
      }
    }

  /** Create the model from ModelMetadata */
  def create(metadata: ModelMetadata): Either[String, Model[RECORD, RESULT]] =
    try {
      getFactory(metadata).create(metadata)
    } catch {
      case scala.util.control.NonFatal(th) => Left(errorMessage(metadata, th))
    }

  private def errorMessage(metadata: ModelMetadata, th: Throwable) =
    s"ModelMetadata.toModel(metadata = $metadata): failed with exception $th. ${formatStackTrace(th)}"
  private def errorMessage(metadata: ModelMetadata, msg: String) =
    s"ModelMetadata.toModel(metadata = $metadata): $msg."

  private def formatStackTrace(th: Throwable): String =
    th.getStackTrace().mkString("\n  ", "\n  ", "\n")

  final case class MisconfiguredFactoryException(
    message: String, cause: Throwable = null) extends RuntimeException(message, cause)

  private def getFactory(metadata: ModelMetadata): ModelFactory[RECORD, RESULT] =
    resolver.getFactory(metadata) match {
      case Some(factory) => factory
      case None =>
        throw MisconfiguredFactoryException(
          errorMessage(metadata, "No factory found for metadata"))
    }
}

