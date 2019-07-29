package com.lightbend.modelserving.model

import pipelinesx.logging.{ LoggingUtil, StdoutStderrLogger }
import scala.concurrent.duration._

trait Model[INRECORD, OUTRECORD] {
  val descriptor: ModelDescriptor

  /** Score a record with the model and update the running statistics. */
  def score(record: INRECORD, stats: ModelServingStats): (OUTRECORD, ModelServingStats)

  /** Hook for cleaning up resources */
  def cleanup(): Unit = {}
}

/**
 * Declares methods required by [[ModelBase]] that concrete classes must implement.
 * The reason this trait is separate is to make it easier to implement some or all
 * of these methods with reusable mixins. For example, 'initFrom' will often be
 * the same for different models in the same application.
 */
trait ModelImplTrait[INRECORD, MODEL_OUTPUT, OUTRECORD] extends Model[INRECORD, OUTRECORD] {

  /**
   * Do implementation-dependent scoring. Note that return type; we considered
   * using Either[String, MODEL_OUTPUT], but in fact you may want as much of the same
   * information as possible sent downstream, whether or not errors occur. The
   * stringent typing also encourages this policy. Hence, _a MODEL_OUTPUT must also
   * always be returned_, but it's value maybe meaningless; a non-empty String
   * indicates that case.
   * @param the input record to score
   * @return a tuple with 1) any errors as a string, so "" when no errors, 2) the
   *         optional score value (which could be an object), possibly None on failure
   */
  protected def invokeModel(input: INRECORD): (String, Option[MODEL_OUTPUT])

  /**
   * Because the OUTRECORDs need to be defined with Avro, which has no support
   * for inheritance among other limitations, and because the OUTRECORD format
   * is unknown in this generic code, it is necessary for you to implement this
   * method that creates the OUTRECORD from the INRECORD. We recommend that the
   * OUTRECORD contain all the fields of the INRECORD, i.e., OUTRECORD <: INRECORD.
   * @param input the original input record used for scoring.
   */
  protected def initFrom(record: INRECORD): OUTRECORD

  /**
   * Because the OUTRECORDs need to be defined with Avro, which has no support
   * for inheritance among other limitations, and because the OUTRECORD format
   * is unknown in this generic code, it is necessary for you to implement this
   * method that adds the scoring results and metadata to the OUTRECORD.
   * See [[ScoreMetadata]] for details of its contents.
   * This method is called in a protocol of first scoring, then partial construction
   * with with INRECORD fields and finally this step that adds the score and metadata.
   * Note that this function is called even when scoring fails, so that as much information
   * as possible can be sent downstream. (Users can optionally filter on failures
   * in a downstream streamlet.) Hence, the score passed in is actually an Option.
   * @param out the partially constructed output record, which has already been initialized with INRECORD fields.
   * @param score the score result. An option in case scoring failed!!
   * @param metadata the score metadata.
   */
  protected def setScoreAndMetadata(
      out: OUTRECORD, score: Option[MODEL_OUTPUT], metadata: ScoreMetadata): OUTRECORD
}

/**
 * Generic definition of an operational machine learning model in memory, which
 * was constructed using the [[ModelDescriptor]] field.
 * `INRECORD` is the type of the records sent to the model for scoring.
 * `OUTRECORD` is the type of the data returned by the model. It should include any
 * fields from the INRECORD needed downstream with the results.
 * MODEL_OUTPUT is the type of results the actual model implementation returns,
 * e.g., a "score".
 * @param descriptor the [[ModelDescriptor]] used to construct this instance.
 * @param logger the logger to use.
 */
abstract class ModelBase[INRECORD, MODEL_OUTPUT, OUTRECORD](
    val descriptor: ModelDescriptor)
  extends Model[INRECORD, OUTRECORD]
  with ModelImplTrait[INRECORD, MODEL_OUTPUT, OUTRECORD] {

  val logger = StdoutStderrLogger(this.getClass()) // make configurable...

  /**
   * Score a record with the model
   * @return the OUTRECORD, including the error string, and some scoring metadata.
   */
  def score(record: INRECORD, stats: ModelServingStats): (OUTRECORD, ModelServingStats) = {
    val start = System.currentTimeMillis()
    var scoreMetadata = ScoreMetadata.init(
      descriptor = descriptor,
      startTime = start.milliseconds)
    val (errors, scoreOption) = try {
      invokeModel(record)
    } catch {
      case scala.util.control.NonFatal(th) ⇒
        (s"score() failed: ${LoggingUtil.throwableToString(th)}", None)
    }
    val duration = (System.currentTimeMillis() - start).milliseconds
    scoreMetadata = scoreMetadata.copy(
      duration = duration,
      errors = errors)

    val out1: OUTRECORD = initFrom(record)
    val out = setScoreAndMetadata(out1, scoreOption, scoreMetadata)
    stats.incrementUsage(duration)
    (out, stats)
  }
}

object Model {
  protected val noopDescription = "Noop model - there is no real model currently available."

  /**
   * Model Descriptor for NOOP models. Note that the bytes are initialized to an
   * empty array in a Some, rather than a None, which is used as the default in
   * [[ModelDescriptorUtil.unknown]], because our TensorFlow model code asserts
   * on None!
   * NOTE: The model type must remain `UNKNOWN`, as that's used as a signal for
   * construction of NoopModels.
   */
  val noopModelDescriptor = ModelDescriptorUtil.unknown.copy(
    modelName = "NoopModel",
    description = noopDescription,
    modelBytes = Some(Array()))

  /**
   * This trait is used to implement "NOOP" implementations of models, where most of
   * the functionality, such as the input and output records are managed normally,
   * but scoring is stubbed out. Hence, this trait has to use a type parameter
   * for the model output type, which is normally hidden by the Model interface.
   */
  trait NoopModel[INRECORD, MODEL_OUTPUT, OUTRECORD] {

    /**
     * Used in concrete NoopModel implementations to override `invokeModel` so that
     * it calls this method and does nothing else. For example, a `ConcreteModel`
     * with type parameters `IN` and `OUT`, and model (score) output of `Double`:
     * ```
     * lazy val noopModel: Model[IN, OUT] =
     *   new ConcreteModel(Model.noopModelDescriptor) with Model.NoopModel[IN, Double, OUT] {
     *     override protected def invokeModel(record: IN): (String, Option[Double]) =
     *       noopInvokeModel(record)
     * }
     * ```
     */
    protected def noopInvokeModel(input: INRECORD): (String, Option[MODEL_OUTPUT]) =
      (noopDescription, None)
  }
}
