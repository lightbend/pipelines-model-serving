package com.lightbend.modelserving.model

import akka.actor.ActorRef

trait ActorResolver {
  /** Retrieve the model using an Int corresponding to the ModelType.value field */
  def getActor(whichActor: String): Option[ActorRef]
  def getActors(): Seq[String]
}
