package com.lightbend.modelserving.model

import pipelines.examples.data.ModelType

/**
 * Generic definition of a machine learning model
 */
trait Model[RECORD, RESULT] {
  /** Score a record with the model */
  def score(input: RECORD): RESULT

  /** Abstraction for cleaning up resources */
  def cleanup(): Unit

  /** Serialize the model to bytes */
  def toBytes(): Array[Byte]

  /** Get the type of model. */
  def getType: ModelType
}
