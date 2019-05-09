package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import com.lightbend.modelserving.model._

/**
 * Actor that handles messages to update a model and to score records using the current model.
 * @param dataType indicating either the record type or model parameters. Used as a file name.
 */
class ModelServingActor[RECORD, RESULT](dataType: String) extends Actor {

  println(s"Creating model serving actor $dataType")
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

    case record: DataToServe[RECORD] =>
      // Process data
      currentModel match {
        case Some(model) =>
          val start = System.currentTimeMillis()
          val quality = model.score(record.getRecord)
          val duration = System.currentTimeMillis() - start
          currentState = currentState.map(_.incrementUsage(duration))
          sender() ! ServingResult(currentState.get.name, record.getType, duration, Some(quality))

        case None =>
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
