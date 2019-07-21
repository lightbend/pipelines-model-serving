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
 * Encapsulates a model to serve along with some metadata about it.
 * Using an Int for the modelType, instead of a ModelDescriptor.ModelType, which is what it represents, is
 * unfortunately necessary because otherwise you can't use these objects in Spark UDFs; you get a Scala Reflection
 * exception at runtime. Hence, the integration values for modelType should match the known integer values in the
 * ModelType objects. See also protobufs/src/main/protobuf/modeldescriptor.proto
 */
final case class ModelToServe(
  name: String,
  description: String,
  modelType: Int,
  model: Array[Byte],
  location: String,
  dataType: String)

/**
 * Model serving statistics definition
 */
final case class ModelToServeStats(
  name: String = "",
  description: String = "",
  modelType: Int = ModelType.PMML.ordinal(),
  since: Long = 0,
  var usage: Long = 0,
  var duration: Double = .0,
  var min: Long = Long.MaxValue,
  var max: Long = Long.MinValue) {

  /**
   * Increment model serving statistics; invoked after scoring every record.
   * @param executionTime Long value for the milliseconds it took to score the record.
   */
  def incrementUsage(executionTime: Long): ModelToServeStats = {
    usage = usage + 1
    duration = duration + executionTime
    if (executionTime < min) min = executionTime
    if (executionTime > max) max = executionTime
    this
  }
}

object ModelToServeStats {
  def apply(m: ModelToServe): ModelToServeStats = ModelToServeStats(m.name, m.description, m.modelType, System.currentTimeMillis())
}
