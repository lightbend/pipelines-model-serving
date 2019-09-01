package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import akka.event.Logging
import com.lightbend.modelserving.model._
import com.lightbend.modelserving.model.persistence.ModelPersistence
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
import org.apache.avro.specific.SpecificRecordBase
import scala.concurrent.duration._

/**
 * Actor that handles messages to update a model and to score records using the current model.
 * @param label used for identifying the app, e.g., as part of a file name for persistence of the current model.
 * @param modelFactory is used to create new models on demand, based on input `ModelDescriptor` instances.
 * @param modelPersistence stores the state durably in case of restart.
 * @param makeDefaultModelOutput produces default "output" for a model.
 */
class ModelServingActor[RECORD, MODEL_OUTPUT](
    label:                  String,
    modelFactory:           ModelFactory[RECORD, MODEL_OUTPUT],
    modelPersistence:       ModelPersistence[RECORD, MODEL_OUTPUT],
    makeDefaultModelOutput: () ⇒ MODEL_OUTPUT) extends Actor {

  val log = Logging(context.system, this)
  log.info(s"Creating ModelServingActor for $label")

  protected var currentModel: Option[Model[RECORD, MODEL_OUTPUT]] = None
  protected var currentStats: ModelServingStats = ModelServingStats.unknown

  override def preStart {
    // check first to see if there's anything to restore...
    if (modelPersistence.stateExists()) {
      modelPersistence.restoreState() match {
        case Right(model) ⇒
          currentModel = Some(model)
          currentStats = ModelServingStats(
            modelType = model.descriptor.modelType,
            modelName = model.descriptor.modelName,
            description = model.descriptor.description)
          log.info(s"Restored model of type $label")
        case Left(unsuccessfulMessage) ⇒
          log.warning(unsuccessfulMessage)
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case descriptor: ModelDescriptor ⇒
      log.info(s"Received new model from descriptor: ${descriptor.toRichString}...")

      modelFactory.create(descriptor) match {
        case Right(newModel) ⇒
          // Log old model stats and clean up, if necessary:
          log.info(s"  Previous model statistics: $currentStats")
          currentModel.map(_.cleanup())
          // Update current model and reset state
          currentModel = Some(newModel)
          currentStats = ModelServingStats(newModel.descriptor)
          // persist new model
          modelPersistence.saveState(newModel) match {
            case Left(error)  ⇒ log.error(error)
            case Right(true)  ⇒ log.info(s"Successfully saved state for model $newModel using location $label")
            case Right(false) ⇒ log.error(s"BUG: ModelPersistence.saveState returned Right(false) for model $newModel.")
          }
        case Left(error) ⇒
          log.error(s"  Failed to instantiate the model: $error")
      }
      sender() ! Done

    // The typing in the the next two lines is a hack. If we have `case r: RECORD`,
    // the compiler complains that it can't check the type of RECORD (it could be
    // a Seq[_] for all it knows, and hence eliminated by erasure), but
    // SpecificRecordBase is a concrete type.
    case recordBase: SpecificRecordBase ⇒
      val record = recordBase.asInstanceOf[RECORD]
      val mr: Model.ModelReturn[MODEL_OUTPUT] = currentModel match {
        case Some(model) ⇒ model.score(record, currentStats)
        case None ⇒
          log.debug("No model is currently available for scoring")
          Model.ModelReturn[MODEL_OUTPUT](
            makeDefaultModelOutput(),
            new ModelResultMetadata(),
            currentStats.incrementUsage(0.milliseconds))
      }
      currentStats = mr.modelServingStats
      if (currentStats.scoreCount % 100 == 0) {
        log.debug(s"Current statistics: $currentStats")
      }
      sender() ! mr

    case _: GetState ⇒
      sender() ! currentStats

    case unknown ⇒
      log.error(s"ModelServingActor: Unknown actor message received: $unknown")
  }
}

object ModelServingActor {

  def props[RECORD, MODEL_OUTPUT](
      label:                  String,
      modelFactory:           ModelFactory[RECORD, MODEL_OUTPUT],
      modelPersistence:       ModelPersistence[RECORD, MODEL_OUTPUT],
      makeDefaultModelOutput: () ⇒ MODEL_OUTPUT): Props =
    Props(new ModelServingActor[RECORD, MODEL_OUTPUT](label, modelFactory, modelPersistence, makeDefaultModelOutput))
}

/** Used as an Actor message. */
case class GetState(label: String)
