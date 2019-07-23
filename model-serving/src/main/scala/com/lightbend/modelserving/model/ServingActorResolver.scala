package com.lightbend.modelserving.model

import akka.actor.ActorRef

/**
 * Resolves which actor to which to forward messages.
 * @param actors the keyed actors for routing
 * @param default the optional default actor to call if none of the others match.
 */
final case class ServingActorResolver(
    actors:  Map[ModelType, ActorRef],
    default: Option[ActorRef]      = None) {

  // For faster runtime performance, precompute whether or not the default is available.
  private val getActorOrDefault: String ⇒ Option[ActorRef] =
    whichActor ⇒ Some(actors.getOrElse(whichActor, default.get))
  private val getActorWithoutDefault: String ⇒ Option[ActorRef] =
    whichActor ⇒ actors.get(whichActor)
  private val get: String ⇒ Option[ActorRef] =
    if (default == None) getActorWithoutDefault else getActorOrDefault

  /**
   * Retrieve the model using a string that functions as a lookup key.
   * @return Some(found actorRef) or Some(default), if default is defined, or None
   */
  def getActor(whichActor: String): Option[ActorRef] = get(whichActor)

  def getActors(): Seq[String] = actors.keys.toSeq
}
