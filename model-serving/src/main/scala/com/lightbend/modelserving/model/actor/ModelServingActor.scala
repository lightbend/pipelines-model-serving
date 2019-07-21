package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import akka.event.Logging
import com.lightbend.modelserving.model._
import com.lightbend.modelserving.model.persistence.FilePersistence

/**
 * Actor that handles messages to update a model and to score records using the current model.
 * @param dataType indicating either the record type or model parameters. Used as a file name.
 */
class ModelServingActor[RECORD, RESULT](dataType: String, modelManager: ModelManager[RECORD, RESULT]) extends Actor {

  val log = Logging(context.system, this)
  log.info(s"Creating model serving actor for $dataType")

  private var currentModel: Option[Model[RECORD, RESULT]] = None
  var currentState: Option[ModelToServeStats] = None

  override def preStart {
    FilePersistence.restoreState[RECORD, RESULT](dataType) match {
      case Right((model, name, description)) =>
        currentModel = Some(model)
        currentState = Some(ModelToServeStats(name, description, model.getType.ordinal(), System.currentTimeMillis()))
        log.info(s"Restored model $name - $description")
      case Left(error) =>
        log.error(error)
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case model: ModelToServe =>
      // Update model
      log.info(s"Received new model: $model")

      modelManager.toModel(model) match {
        case Right(m) =>
          // close current model first
          currentModel.foreach(_.cleanup())
          // Update model and state
          currentModel = Some(m)
          currentState = Some(ModelToServeStats(model))
          // persist new model
          FilePersistence.saveState(dataType, m, model.name, model.description) match {
            case Left(error) => log.error(error)
            case Right(true) => log.info(s"Successfully saved state for model type $dataType, model name = ${model.name}")
            case Right(false) => log.error("BUG: FilePersistence.saveState returned Right(false).")
          }
        case Left(error) =>
          log.error(s"Failed to instantiate the model: $error")
      }
      sender() ! Done

    case record: DataToServe =>
      // Process data
      currentModel match {
        case Some(model) =>
          val start = System.currentTimeMillis()
          val prediction = model.score(record.getRecord)
          val duration = System.currentTimeMillis() - start
          currentState = currentState.map(_.incrementUsage(duration))
          log.info(s"Processed data in $duration ms with result $prediction")
          sender() ! ServingResult(currentState.get.name, record.getType, duration, Some(prediction))

        case None =>
          log.warning(s"no model skipping")
          sender() ! ServingResult("No model available")
      }

    case _: GetState => {
      // State query
      sender() ! currentState.getOrElse(ModelToServeStats())
    }
  }
}

object ModelServingActor {
  def props[RECORD, RESULT](
    dataType: String,
    modelManager: ModelManager[RECORD, RESULT]): Props =
    Props(new ModelServingActor[RECORD, RESULT](dataType, modelManager))
}

/** Used as an Actor message. */
case class GetState(dataType: String)
