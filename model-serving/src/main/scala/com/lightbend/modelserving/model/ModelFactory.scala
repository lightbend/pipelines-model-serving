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
 * Generic definition of a model factory
 */
trait ModelFactory[RECORD, RESULT] {

  /**
   * Define this method for concrete subclasses.
   * It's protected; end users call "create", while implementers define "make".
   */
  protected def make(descriptor: ModelDescriptor): Model[RECORD, RESULT]

  def create(descriptor: ModelDescriptor): Either[String, Model[RECORD, RESULT]] =
    try {
      Right(make(descriptor))
    } catch {
      case scala.util.control.NonFatal(th) ⇒
        val thMsg = LoggingUtil.throwableToStrings(th).mkString("\n")
        Left(s"Model factory failed to create a model from descriptor $descriptor. $thMsg")
    }
}
