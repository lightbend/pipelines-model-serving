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

import pipelinesx.logging.LoggingUtil

/**
 * Used to construct new models, persist existing ones, and reconstitute persisted models.
 */
final class ModelManager[RECORD, RESULT](resolver: ModelFactoryResolver[RECORD, RESULT]) {

  override def toString: String = super.toString

  /** Create the model from ModelDescriptor */
  def create(descriptor: ModelDescriptor): Either[String, Model[RECORD, RESULT]] =
    try {
      getFactory(descriptor).create(descriptor)
    } catch {
      case scala.util.control.NonFatal(th) ⇒ Left(errorMessage(descriptor, th))
    }

  private def errorMessage(descriptor: ModelDescriptor, th: Throwable) = {
    val thStr = LoggingUtil.throwableToString(th)
    s"ModelDescriptor.toModel(descriptor = $descriptor): failed with exception $thStr"
  }

  private def errorMessage(descriptor: ModelDescriptor, msg: String) =
    s"ModelDescriptor.toModel(descriptor = $descriptor): $msg."

  private def getFactory(descriptor: ModelDescriptor): ModelFactory[RECORD, RESULT] =
    resolver.getFactory(descriptor) match {
      case Some(factory) ⇒ factory
      case None ⇒
        throw ModelManager.MisconfiguredFactoryException(
          errorMessage(descriptor, "No factory found for descriptor"))
    }
}

object ModelManager {
  final case class MisconfiguredFactoryException(
      message: String, cause: Throwable = null) extends RuntimeException(message, cause)
}
