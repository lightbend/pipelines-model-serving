package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, ActorLogging, Props }
import akka.cluster.ddata.Replicator.{ Changed, Subscribe, Update, WriteMajority }
import akka.cluster.ddata.{ DistributedData, LWWMap, LWWMapKey }
import com.lightbend.modelserving.model._

import scala.concurrent.duration._

/**
 * Actor that handles messages to update a model and to score records using the current model.
 * @param dataType indicating either the record type or model parameters. Used as a file name.
 */
class ModelServingActor[RECORD, RESULT] extends Actor with ActorLogging {

  implicit val node = DistributedData(context.system).selfUniqueAddress
  val replicator = DistributedData(context.system).replicator
  val ModelKey = LWWMapKey[String, ModelToServe]("MLModels")

  private var currentModel: Option[Model[RECORD, RESULT]] = None
  var currentState: Option[ModelToServeStats] = None

  replicator ! Subscribe(ModelKey, self)

  override def receive: PartialFunction[Any, Unit] = {
    case model: ModelToServe => {
      // Update model
      println(s"Updated model: $model")

      updateModel(model)

      val writeMajority = WriteMajority(timeout = 5.seconds)
      replicator ! Update(ModelKey, LWWMap.empty[String, ModelToServe], writeMajority)(_ :+ (self.path.name, model))

      sender() ! Done
    }

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

    case c @ Changed(ModelKey) => {
      val model = c.get(ModelKey).get(self.path.name).get
      log.info("Received Distributed Data Model Update: " + model.name + " " + model.description)
      updateModel(model)
    }

    case _: GetState => {
      // State query
      sender() ! currentState.getOrElse(ModelToServeStats())
    }
  }

  def updateModel(model: ModelToServe): Unit = {

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
  }
}

object ModelServingActor {
  def props[RECORD, RESULT](): Props = Props(new ModelServingActor[RECORD, RESULT])
}

/** Used as an Actor message. */
case class GetState(dataType: String)
