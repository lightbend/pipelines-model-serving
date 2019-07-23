package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import akka.event.Logging
import com.lightbend.modelserving.model._

/**
 * Router actor, which routes both model and data (records) to an appropriate actor.
 * Based on http://michalplachta.com/2016/01/23/scalability-using-sharding-from-akka-cluster/
 */
class ModelServingManager(actorResolver: ServingActorResolver) extends Actor {

  val log = Logging(context.system, this)
  log.info(s"Creating model serving manager")

  override def receive: PartialFunction[Any, Unit] = {
    case descriptor: ModelDescriptor ⇒ // new model
      actorResolver.getActor(descriptor.modelType.toString) match {
        case Some(modelServer) ⇒
          log.info(s"forwarding model descriptor $descriptor to $modelServer")
          modelServer forward descriptor
        case _ ⇒
          log.error(s"no model server found for descriptor $descriptor. Skipping...")
          sender() ! Done
      }

    case record: DataToServe[_] ⇒ // data to score with existing model(s)
      actorResolver.getActor(record.getType) match {
        case Some(modelServer) ⇒
          //          log.info(s"forwarding data request to $modelServer")
          modelServer forward record
        case _ ⇒
          log.error(s"no model server found for type ${record.getType}. Skipping...")
          sender() ! ServingResult("No model server available")
      }

    case getState: GetState ⇒ actorResolver.getActor(getState.label) match {
      case Some(server) ⇒ server forward getState
      case _            ⇒ sender() ! ModelServingStats.unknown
    }

    case _: GetModels ⇒ sender() ! GetModelsResult(actorResolver.getActors())
  }
}

object ModelServingManager {
  def props(actorResolver: ServingActorResolver): Props = Props(new ModelServingManager(actorResolver))
}

/** Used as an Actor message. */
case class GetModels()

/** Used as a message returned for the GetModels request. */
case class GetModelsResult(models: Seq[String])
