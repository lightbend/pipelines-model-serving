package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import akka.event.Logging
import com.lightbend.modelserving.model._
import com.lightbend.modelserving.model.persistence.FilePersistence
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

/**
 * Actor that handles messages to update a model and to score records using the current model.
 * @param label used as a key to determine when to use this model and also as part of a file name for persistence of it.
 */
class ModelServingActor[RECORD, RESULT](label: String, modelManager: ModelManager[RECORD, RESULT]) extends Actor {

  val log = Logging(context.system, this)
  log.info(s"Creating ModelServingActor for $label")

  private val filePersistence = FilePersistence[RECORD, RESULT](modelManager)

  private var currentModel: Option[Model[RECORD, RESULT]] = None
  var currentState: Option[ModelServingStats] = None

  override def preStart {
    // check first to see if there's anything to restore...
    if (filePersistence.stateExists(label)) {
      filePersistence.restoreState(label) match {
        case Right(model) ⇒
          currentModel = Some(model)
          currentState = Some(
            ModelServingStats(
              modelType = model.descriptor.modelType,
              name = model.descriptor.name,
              description = model.descriptor.description,
              since = System.currentTimeMillis()))
          log.info(s"Restored model with descriptor ${model.descriptor}")
        case Left(error) ⇒
          log.error(error)
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case descriptor: ModelDescriptor ⇒
      log.info(s"Received new model from descriptor: $descriptor")

      modelManager.create(descriptor) match {
        case Right(newModel) ⇒
          // close current model first
          currentModel.foreach(_.cleanup())
          // Update model and state
          currentModel = Some(newModel)
          currentState = Some(ModelServingStats(newModel.descriptor))
          // persist new model
          filePersistence.saveState(newModel, descriptor.constructName()) match {
            case Left(error)  ⇒ log.error(error)
            case Right(true)  ⇒ log.info(s"Successfully saved state for model $newModel")
            case Right(false) ⇒ log.error(s"BUG: FilePersistence.saveState returned Right(false) for model $newModel.")
          }
        case Left(error) ⇒
          log.error(s"Failed to instantiate the model: $error")
      }
      sender() ! Done

    case record: DataToServe[RECORD] ⇒
      currentModel match {
        case Some(model) ⇒
          val start = System.currentTimeMillis()
          val prediction = model.score(record.record)
          val duration = System.currentTimeMillis() - start
          currentState = currentState.map(_.incrementUsage(duration))
          log.info(s"Processed data in $duration ms with result $prediction")
          sender() ! ServingResult(currentState.get.name, record.getType, duration, Some(prediction))

        case None ⇒
          log.warning(s"no model skipping")
          sender() ! ServingResult("No model available")
      }

    case _: GetState ⇒
      sender() ! currentState.getOrElse(ModelServingStats.unknown)

    case unknown ⇒
      log.error(s"ModelServingActor: Unknown actor message received: $unknown")
  }
}

object ModelServingActor {
  def props[RECORD, RESULT](
      label:        String,
      modelManager: ModelManager[RECORD, RESULT]): Props =
    Props(new ModelServingActor[RECORD, RESULT](label, modelManager))
}

/** Used as an Actor message. */
case class GetState(label: String)
