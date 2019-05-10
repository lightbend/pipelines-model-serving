package pipelines.examples.modelserving

import akka.actor.ActorRef
import com.lightbend.modelserving.model.ActorResolver

/**
 * Model factory resolver - requires specific factories
 */
object ServingActorResolver extends ActorResolver {

  var actors: Map[String, ActorRef] = _

  def setActors(a: Map[String, ActorRef]): Unit = actors = a

  /** Retrieve the model using an Int corresponding to the ModelType.value field */
  override def getActor(whichActor: String): Option[ActorRef] = actors.get(whichActor)

  override def getActors(): Seq[String] = actors.keys.toSeq
}
