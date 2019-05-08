package com.lightbend.modelserving.model.actor

import akka.actor.{Actor, ActorRef, Props}
import com.lightbend.modelserving.model.{DataToServe, ModelToServe, ModelToServeStats}

/**
  * Router actor, which routes both model and data (records) to an appropriate actor
  * Based on http://michalplachta.com/2016/01/23/scalability-using-sharding-from-akka-cluster/
  */
class ModelServingManager[RECORD, RESULT] extends Actor {

  private def getModelServer(dataType: String): ActorRef = {
    context.child(dataType).getOrElse(context.actorOf(ModelServingActor.props[RECORD, RESULT](dataType), dataType))
  }

  private def getInstances : GetModelsResult =
    GetModelsResult(context.children.map(_.path.name).toSeq)

  override def receive: PartialFunction[Any, Unit] = {
    case model: ModelToServe => getModelServer(model.dataType) forward model

    case record: DataToServe[RECORD] => getModelServer(record.getType) forward record

    case getState: GetState => context.child(getState.dataType) match{
      case Some(server) => server forward getState
      case _ => sender() ! ModelToServeStats()
    }

    case _ : GetModels => sender() ! getInstances
  }
}

object ModelServingManager{
  def props[RECORD, RESULT] : Props = Props(new ModelServingManager[RECORD, RESULT]())
}

/** Used as an Actor message. */
case class GetModels()

/** Used as a message returned for the GetModels request. */
case class GetModelsResult(models : Seq[String])
