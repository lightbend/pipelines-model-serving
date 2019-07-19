package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import com.lightbend.modelserving.model._

/**
 * Actor that handles messages to update a model and to score records using the current model.
 * @param dataType indicating either the record type or model parameters. Used as a file name.
 */
class ModelServingActor[RECORD, RESULT] extends Actor {

  println("Creating model serving actor for wine")
  private var currentModel: Option[Model[RECORD, RESULT]] = None
  var currentState: Option[ModelToServeStats] = None

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
        case _ => // Failed converting
          println(s"Failed to convert model: ${model.model}")
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
  def props[RECORD, RESULT](): Props = Props(new ModelServingActor[RECORD, RESULT])
}

/** Used as an Actor message. */
case class GetState(dataType: String)
