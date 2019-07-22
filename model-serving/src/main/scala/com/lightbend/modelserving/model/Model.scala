package com.lightbend.modelserving.model

/**
 * Generic definition of a machine learning model
 */
trait Model[RECORD, RESULT] {
  /**
   * Score a record with the model
   * @return either an error string or the result.
   */
  def score(input: RECORD): Either[String, RESULT]

  /** Abstraction for cleaning up resources */
  def cleanup(): Unit

  /** The metadata about the model including the model type. */
  val metadata: ModelMetadata
}
