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
 * Base interface for ModelFactories resolver. The implementation of this trait should return the model factory
 * base on a model type, an enumeration defined in ModelDescription.avsc.
 */
trait ModelFactoryResolver[RECORD, RESULT] {

  /** Retrieve a model using metadata for it. */
  def getFactory(descriptor: ModelDescriptor): Option[ModelFactory[RECORD, RESULT]]
}
