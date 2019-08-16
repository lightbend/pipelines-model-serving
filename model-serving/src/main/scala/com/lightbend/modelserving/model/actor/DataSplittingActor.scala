package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import akka.event.Logging
import com.lightbend.modelserving.model.persistence.FilePersistence
import com.lightbend.modelserving.model.splitter.{ GetWithProbability, StreamSplitterUtil }
import com.lightbend.modelserving.splitter.StreamSplitter
import org.apache.avro.specific.SpecificRecordBase

/**
 * Actor that handles messages to update a splitting policy and to split input according to the percentages.
 * @param label used for identifying the app, e.g., as part of a file name for persistence of the split policy.
 */
class DataSplittingActor(
    label: String) extends Actor {

  val log = Logging(context.system, this)
  log.info(s"Creating SplitterActor for $label")

  protected val filePersistence = FilePersistence.apply(null)

  protected var currentTransformer: Option[StreamSplitter] = None
  protected var boundaries: Array[Int] = _
  protected var outlets: Array[Int] = _

  override def preStart {
    // check first to see if there's anything to restore...
    if (filePersistence.stateExists(label)) {
      filePersistence.restoreSplitState(label) match {
        case Right(splitter) ⇒
          currentTransformer = Some(splitter)
          val definition = StreamSplitterUtil.splitdefinition(splitter)
          outlets = definition._1
          boundaries = GetWithProbability.calculateBoundaries(definition._2)
          log.info(s"Restored model of type $label")
        case Left(unsuccessfulMessage) ⇒
          log.warning(unsuccessfulMessage)
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = {

    // Process a new splitting definition
    case splitter: StreamSplitter ⇒
      log.info(s"Received new splitter: ${splitter}...")

      currentTransformer = Some(splitter)
      val definition = StreamSplitterUtil.splitdefinition(splitter)
      outlets = definition._1
      boundaries = GetWithProbability.calculateBoundaries(definition._2)
      // persist new splitter
      filePersistence.saveState(splitter, label) match {
        case Left(error)  ⇒ log.error(error)
        case Right(true)  ⇒ log.info(s"Successfully saved state for splitter $splitter using location $label")
        case Right(false) ⇒ log.error(s"BUG: FilePersistence.saveState returned Right(false) for splitter $splitter.")
      }
      sender() ! Done

    // Calculate outlet for a message, input one is used if there is no splitter
    case request: RecordWithOutlet ⇒
      val outlet = currentTransformer match {
        case Some(t@_) ⇒ RecordWithOutlet(outlets(GetWithProbability.choseOnebounded(boundaries)), request.record)
        case None ⇒
          log.debug("No splitting information is currently available for scoring")
          request
      }
      sender() ! outlet
  }
}

object DataSplittingActor {

  def props(label: String): Props = Props(new DataSplittingActor(label))
}

case class RecordWithOutlet(outlet: Int, record: SpecificRecordBase)
