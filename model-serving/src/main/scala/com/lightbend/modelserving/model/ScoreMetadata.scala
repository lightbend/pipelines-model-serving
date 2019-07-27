package com.lightbend.modelserving.model

import scala.concurrent.duration._

/**
 * Metadata about the score "event". To make it easier to add new information to
 * this object, such as quality metrics, this class and [[Model]] try to limit
 * the details to their view. Also, this class provides a Map for storing ad hoc
 * information without having to modify this class, but the OUTRECORD type used
 * in subclasses of [[Model]] will have to know how to use that information!
 * @param startTime Epoch milliseconds when scoring was started.
 * @param duration How long it took to score the record.
 * @param errors Any errors encountered, as a string, so empty if no errors happened.
 * @param modelName The name of the model
 * @param modelType The type of the model
 */
final case class ScoreMetadata(
    startTime: FiniteDuration = 0.milliseconds,
    duration:  FiniteDuration = 0.milliseconds,
    errors:    String         = "",
    modelName: String         = "Unknown",
    modelType: ModelType      = ModelType.UNKNOWN)

object ScoreMetadata {
  val unknown = new ScoreMetadata()

  /** Construct a partially-complete object with the start time and model metadata. */
  def init(descriptor: ModelDescriptor, startTime: FiniteDuration): ScoreMetadata =
    new ScoreMetadata(
      startTime = startTime,
      modelName = descriptor.modelName,
      modelType = descriptor.modelType)
}
