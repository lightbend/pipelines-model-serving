package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import com.lightbend.modelserving.model._
import com.lightbend.modelserving.model.persistence.FilePersistence

/**
 * Actor that handles messages to update a model and to score records using the current model.
 * @param dataType indicating either the record type or model parameters. Used as a file name.
 */
class ModelServingActor[RECORD, RESULT](dataType: String) extends Actor {

  println("Creating model serving actor for wine")
  private var currentModel: Option[Model[RECORD, RESULT]] = None
  var currentState: Option[ModelToServeStats] = None

  override def preStart {
    FilePersistence.restoreState(dataType) match {
      case Some(value) => // manage to restore
        currentModel = Some(value._1.asInstanceOf[Model[RECORD, RESULT]])
        currentState = Some(ModelToServeStats(value._2, value._3, value._1.getType.ordinal(), System.currentTimeMillis()))
        println(s"Restored model ${value._2} - ${value._3}")
      case _ =>
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case model: ModelToServe =>
      // Update model
      println(s"Updated model: $model")

      ModelToServe.toModel[RECORD, RESULT](model) match {
        case Some(m) => // Successfully got a new model
          // close current model first
          currentModel.foreach(_.cleanup())
          // Update model and state
          currentModel = Some(m)
          currentState = Some(ModelToServeStats(model))
          // persist new model
          FilePersistence.saveState(dataType, m, model.name, model.description)
        case _ => // Failed converting
          println(s"Failed to convert model: $model")
      }
      sender() ! Done

    case record: DataToServe =>
      // Process data
      currentModel match {
        case Some(model) =>
          val start = System.currentTimeMillis()
          val prediction = model.score(record.getRecord.asInstanceOf[RECORD])
          val duration = System.currentTimeMillis() - start
          currentState = currentState.map(_.incrementUsage(duration))
          println(s"Processed data in $duration ms with result $prediction")
          sender() ! ServingResult(currentState.get.name, record.getType, duration, Some(prediction))

        case None =>
          println(s"no model skipping")
          sender() ! ServingResult("No model available")
      }

    case _: GetState => {
      // State query
      sender() ! currentState.getOrElse(ModelToServeStats())
    }
  }
}

object ModelServingActor {
  def props[RECORD, RESULT](dataType: String): Props = Props(new ModelServingActor[RECORD, RESULT](dataType))
}

/** Used as an Actor message. */
case class GetState(dataType: String)
