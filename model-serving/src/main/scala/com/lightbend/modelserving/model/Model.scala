package com.lightbend.modelserving.model

/**
 * Generic definition of an operational machine learning model in memory, which
 * was constructed using the [[ModelDescriptor]] field.
 */
trait Model[RECORD, RESULT] {
  /**
   * Score a record with the model
   * @return either an error string or the result.
   */
  def score(input: RECORD): Either[String, RESULT]

  /** Abstraction for cleaning up resources */
  def cleanup(): Unit

  /** The descriptor about the model. */
  val descriptor: ModelDescriptor
}
