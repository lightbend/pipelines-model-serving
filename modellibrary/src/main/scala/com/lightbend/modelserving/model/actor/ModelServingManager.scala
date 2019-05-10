package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import com.lightbend.modelserving.model._

/**
 * Router actor, which routes both model and data (records) to an appropriate actor
 * Based on http://michalplachta.com/2016/01/23/scalability-using-sharding-from-akka-cluster/
 */
class ModelServingManager(actorResolver: ActorResolver) extends Actor {

  println(s"Creating model serving manager")

  override def receive: PartialFunction[Any, Unit] = {
    case model: ModelToServe =>
      actorResolver.getActor(model.dataType) match {
        case Some(modelServer) =>
          //          println(s"forwarding model request to $modelServer")
          modelServer forward model
        case _ =>
          println(s"no model server skipping")
          sender() ! Done
      }

    case record: DataToServe =>
      actorResolver.getActor(record.getType) match {
        case Some(modelServer) =>
          //          println(s"forwarding data request to $modelServer")
          modelServer forward record
        case _ =>
          println(s"no model server skipping")
          sender() ! ServingResult("No model server available")
      }

    case getState: GetState => actorResolver.getActor(getState.dataType) match {
      case Some(server) => server forward getState
      case _ => sender() ! ModelToServeStats()
    }

    case _: GetModels => sender() ! GetModelsResult(actorResolver.getActors())
  }
}

object ModelServingManager {
  def props(actorResolver: ActorResolver): Props = Props(new ModelServingManager(actorResolver))
}

/** Used as an Actor message. */
case class GetModels()

/** Used as a message returned for the GetModels request. */
case class GetModelsResult(models: Seq[String])
