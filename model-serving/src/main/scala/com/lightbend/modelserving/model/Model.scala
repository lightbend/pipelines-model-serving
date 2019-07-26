package com.lightbend.modelserving.model

import pipelinesx.logging.{ LoggingUtil, StdoutStderrLogger }

trait Model[INRECORD, OUTRECORD] {
  val descriptor: ModelDescriptor

  /** Score a record with the model */
  def score(input: INRECORD): ServingResult[OUTRECORD]

  /** Abstraction for cleaning up resources */
  def cleanup(): Unit
}

/**
 * Generic definition of an operational machine learning model in memory, which
 * was constructed using the [[ModelDescriptor]] field.
 * `INRECORD` is the type of the records sent to the model for scoring.
 * `OUTRECORD` is the type of the data returned by the model. It should include any
 * fields from the INRECORD needed downstream with the results.
 * SCORE is the type of results the actual model implementation returns.
 * @param descriptor the [[ModelDescriptor]] used to construct this instance.
 * @param logger the logger to use.
 */
abstract class ModelBase[INRECORD, SCORE, OUTRECORD](
    val descriptor: ModelDescriptor) extends Model[INRECORD, OUTRECORD] {

  val logger = StdoutStderrLogger(this.getClass()) // make configurable...

  /**
   * Do implementation-dependent scoring. Note that return type; we considered
   * using Either[String, SCORE], but in fact you may want as much of the same
   * information as possible sent downstream, whether or not errors occur. The
   * stringent typing also encourages this policy. Hence, _a SCORE must also
   * always be returned_, but it's value maybe meaningless; a non-empty String
   * indicates that case.
   */
  protected def invokeModel(input: INRECORD): (String, SCORE)

  /**
   * Because the OUTRECORDs need to be defined with Avro, which has limitations,
   * like no support for inheritance, and the OUTRECORD format is unknown in this
   * generic code, it is necessary for you to implement this method that creates
   * the OUTRECORD and sets any data required form the metadata passed in as
   * arguments, the result (obviously) and any errors, and any fields from the
   * INRECORD.
   * @param input the original input record used for scoring.
   * @param errors an empty string if no errors occurred, otherwise information about the errors.
   * @param score the output of the model scoring.
   * @param duration the time in milliseconds it took for the model to score the record.
   * @param modelName the name or id of this model.
   * @param modelType the kind of model (e.g., PMML...)
   */
  protected def makeOutRecord(
      record:    INRECORD,
      errors:    String,
      score:     SCORE,
      duration:  Long,
      modelName: String,
      modelType: ModelType): OUTRECORD

  /**
   * Score a record with the model
   * @return the ServingResult with the OUTRECORD, error string, and some scoring metadata.
   */
  def score(record: INRECORD): ServingResult[OUTRECORD] = try {
    val start = System.currentTimeMillis()
    val (errors, score) = invokeModel(record)
    val duration = System.currentTimeMillis() - start
    logger.debug(s"Processed record in $duration ms with result $score")
    val out = makeOutRecord(
      record, errors, score, duration, descriptor.name, descriptor.modelType)
    ServingResult(
      result = Some(out),
      errors = errors,
      duration = duration,
      modelName = descriptor.name,
      modelType = descriptor.modelType)
  } catch {
    case scala.util.control.NonFatal(th) ⇒
      ServingResult(
        result = None,
        errors = s"score() failed: ${LoggingUtil.throwableToString(th)}",
        duration = 0,
        modelName = descriptor.name,
        modelType = descriptor.modelType)

  }
}

object Model {

  /**
   * Model serving result data. Used for actor messaging, primarily.
   */
  case class ServingResult[OUTRECORD](
      result:    Option[OUTRECORD],
      errors:    String            = "",
      modelName: String            = "No Model",
      modelType: ModelType         = ModelType.UNKNOWN,
      duration:  Long              = 0)
}
