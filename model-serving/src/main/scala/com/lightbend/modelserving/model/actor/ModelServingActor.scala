package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import akka.event.Logging
import com.lightbend.modelserving.model._
import com.lightbend.modelserving.model.persistence.FilePersistence
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
import org.apache.avro.specific.SpecificRecordBase

/**
 * Actor that handles messages to update a model and to score records using the current model.
 * @param label used for identifying the app, e.g., as part of a file name for persistence of the current model.
 * @param modelFactory is used to create new models on demand, based on input [[ModelDescriptor]] instances.
 */
class ModelServingActor[RECORD, RESULT](
    label:        String,
    modelFactory: ModelFactory[RECORD, RESULT]) extends Actor {

  val log = Logging(context.system, this)
  log.info(s"Creating ModelServingActor for $label")

  private val filePersistence = FilePersistence[RECORD, RESULT](modelFactory)

  private var currentModel: Option[Model[RECORD, RESULT]] = None
  var currentState: Option[ModelServingStats] = None
  var countRecordsWithNoModel: Int = 0

  override def preStart {
    // check first to see if there's anything to restore...
    if (filePersistence.stateExists(label)) {
      filePersistence.restoreState(label) match {
        case Right(model) ⇒
          currentModel = Some(model)
          currentState = Some(
            ModelServingStats(
              modelType = model.descriptor.modelType,
              modelName = model.descriptor.name,
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
      log.info(s"Received new model from descriptor: ${descriptor.toRichString}...")

      modelFactory.create(descriptor) match {
        case Right(newModel) ⇒
          // Log old model stats:
          if (countRecordsWithNoModel > 0)
            log.info(s"  $countRecordsWithNoModel records weren't scored, because there was no model.")
          currentState.map(s ⇒
            log.info(s"  Old model's stats: $s"))
          // close current model first
          currentModel.foreach(_.cleanup())
          // Update model and state
          currentModel = Some(newModel)
          currentState = Some(ModelServingStats(newModel.descriptor))
          countRecordsWithNoModel = 0
          // persist new model
          filePersistence.saveState(newModel, descriptor.constructName()) match {
            case Left(error)  ⇒ log.error(error)
            case Right(true)  ⇒ log.info(s"Successfully saved state for model $newModel")
            case Right(false) ⇒ log.error(s"BUG: FilePersistence.saveState returned Right(false) for model $newModel.")
          }
        case Left(error) ⇒
          log.error(s"  Failed to instantiate the model: $error")
      }
      sender() ! Done

    // The typing in the these two lines is a hack. If we just have `case r: RECORD`
    // the compiler complains that it can't check the type of RECORD (it could be
    // a Seq[_] for all it knows, and hence eliminated by erasure).
    case recordBase: SpecificRecordBase ⇒
      val record = recordBase.asInstanceOf[RECORD]
      currentModel match {
        case Some(model) ⇒
          sender() ! model.score(record)

        case None ⇒
          countRecordsWithNoModel += 1
          if (countRecordsWithNoModel % 100 == 0)
            log.warning(s"No model available for scoring. $countRecordsWithNoModel records skipped!")
          sender() ! ServingResult(
            errors = s"No model available - missed score count $countRecordsWithNoModel",
            result = None)
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
      modelFactory: ModelFactory[RECORD, RESULT]): Props =
    Props(new ModelServingActor[RECORD, RESULT](label, modelFactory))
}

/** Used as an Actor message. */
case class GetState(label: String)
