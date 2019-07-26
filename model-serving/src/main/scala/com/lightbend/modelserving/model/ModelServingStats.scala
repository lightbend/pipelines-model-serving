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
 * Model serving statistics definition.
 * TODO: Assumes that
 */
final case class ModelServingStats(
    modelType:      ModelType = ModelType.UNKNOWN,
    modelName:      String    = "",
    description:    String    = "",
    since:          Long      = 0,
    var scoreCount: Long      = 0,
    var duration:   Long      = 0,
    var min:        Long      = Long.MaxValue,
    var max:        Long      = Long.MinValue) {

  /**
   * Increment model serving statistics; invoked after scoring every record.
   * @param executionTime Long value for the milliseconds it took to score the record.
   */
  def incrementUsage(executionTime: Long): ModelServingStats = {
    scoreCount = scoreCount + 1
    duration = duration + executionTime
    if (executionTime < min) min = executionTime
    if (executionTime > max) max = executionTime
    this
  }
}

object ModelServingStats {
  def apply(descriptor: ModelDescriptor): ModelServingStats =
    new ModelServingStats(
      modelType = descriptor.modelType,
      modelName = descriptor.name,
      description = descriptor.description,
      since = System.currentTimeMillis())

  val unknown = new ModelServingStats(
    modelType = ModelType.UNKNOWN,
    modelName = "<unknown>",
    description = "No stats have been collected yet!")
}
