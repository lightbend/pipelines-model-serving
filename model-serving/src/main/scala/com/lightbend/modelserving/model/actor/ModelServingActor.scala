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
 * @param modelFactory is used to create new models on demand, based on input [[ModelDescriptor]] instances. WARNING, this factory must support [[Model.noopModelDescriptor]].
 */
class ModelServingActor[RECORD, RESULT](
    label:        String,
    modelFactory: ModelFactory[RECORD, RESULT]) extends Actor {

  val log = Logging(context.system, this)
  log.info(s"Creating ModelServingActor for $label")

  protected val filePersistence = FilePersistence[RECORD, RESULT](modelFactory)

  // Create a NoopModel to initialize the current model, before a real one is ingested.
  lazy val noopModel = modelFactory.create(Model.noopModelDescriptor) match {
    case Right(model) ⇒ model
    case Left(errors) ⇒
      val errorStr = s"modelFactory.create() didn't return a NoopModel (for initialization purposes): errors = $errors"
      log.error(errorStr)
      throw new RuntimeException(errorStr)
  }
  protected var currentModel: Model[RECORD, RESULT] = noopModel
  protected var currentStats: ModelServingStats = ModelServingStats.noop

  override def preStart {
    // check first to see if there's anything to restore...
    if (filePersistence.stateExists(label)) {
      filePersistence.restoreState(label) match {
        case Right(model) ⇒
          currentModel = model
          currentStats = ModelServingStats(
            modelType = model.descriptor.modelType,
            modelName = model.descriptor.modelName,
            description = model.descriptor.description)
          log.info(s"Restored model with descriptor ${model.descriptor}")
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
          // Log old model stats and clean up:
          log.info(s"  Previous model statistics: $currentStats")
          currentModel.cleanup()
          // Update current model and reset state
          currentModel = newModel
          currentStats = ModelServingStats(newModel.descriptor)
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

    // The typing in the the next two lines is a hack. If we have `case r: RECORD`,
    // the compiler complains that it can't check the type of RECORD (it could be
    // a Seq[_] for all it knows, and hence eliminated by erasure), but
    // SpecificRecordBase is a concrete type.
    case recordBase: SpecificRecordBase ⇒
      val record = recordBase.asInstanceOf[RECORD]
      val (result, stats2) = currentModel.score(record, currentStats)
      currentStats = stats2
      if (currentStats.scoreCount % 100 == 0) {
        log.debug(s"Current statistics: $currentStats")
      }
      sender() ! result

    case _: GetState ⇒
      sender() ! currentStats

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
