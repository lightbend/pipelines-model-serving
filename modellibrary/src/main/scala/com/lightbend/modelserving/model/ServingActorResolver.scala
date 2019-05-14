package com.lightbend.modelserving.model

import akka.actor.ActorRef

/**
 * Model factory resolver - requires specific factories
 */
class ServingActorResolver(actors: Map[String, ActorRef]){

  /** Retrieve the model using an Int corresponding to the ModelType.value field */
  def getActor(whichActor: String): Option[ActorRef] = actors.get(whichActor)

  def getActors(): Seq[String] = actors.keys.toSeq
}
