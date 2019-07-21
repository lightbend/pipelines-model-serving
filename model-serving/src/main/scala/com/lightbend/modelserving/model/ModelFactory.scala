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

/**
 * Generic definition of a model factory
 */
trait ModelFactory[RECORD, RESULT] {

  /** Used as a label for the kind of model in error messages. */
  def modelName: String
  def make(input: ModelToServe): Model[RECORD, RESULT]
  def make(bytes: Array[Byte]): Model[RECORD, RESULT]

  def create(input: ModelToServe): Either[String, Model[RECORD, RESULT]] = {
    try {
      Right(make(input.model))
    } catch {
      case scala.util.control.NonFatal(th) ⇒
        Left(s"Failed to create a new $modelName from with the input $input. $th. ${formatStackTrace(th)}")
    }
  }

  def restore(bytes: Array[Byte]): Either[String, Model[RECORD, RESULT]] =
    try {
      Right(make(bytes))
    } catch {
      case scala.util.control.NonFatal(th) ⇒
        Left(s"Failed to restore a $modelName from a byte array. $th. ${formatStackTrace(th)}")
    }

  private def formatStackTrace(th: Throwable): String = th.getStackTrace().mkString("\n  ", "\n  ", "\n")
}
