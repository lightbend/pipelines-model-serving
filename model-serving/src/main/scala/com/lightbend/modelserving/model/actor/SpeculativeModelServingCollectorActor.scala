package com.lightbend.modelserving.model.actor

import akka.Done
import akka.actor.{ Actor, Props }
import akka.event.Logging
import com.lightbend.modelserving.model.persistence.FilePersistence
import com.lightbend.modelserving.model.speculative.{ Decider, SpeculativeRecordSplitter }
import com.lightbend.modelserving.speculative.{ SpeculativeStreamMerger, StartSpeculative }
import org.apache.avro.specific.SpecificRecordBase

import util.control.Breaks._
import scala.collection.mutable.ListBuffer

class SpeculativeModelServingCollectorActor[MODEL_OUTPUT](
    label:          String,
    recordsplitter: SpeculativeRecordSplitter[MODEL_OUTPUT],
    decider:        Decider[MODEL_OUTPUT]) extends Actor {

  val SERVERTIMEOUT = 1000l
  protected val filePersistence = FilePersistence.apply(null)

  val log = Logging(context.system, this)
  log.info(s"Creating Speculative Model serving collector Actor for $label")

  var timeout = SERVERTIMEOUT
  var required = 1

  val currentProcessing = collection.mutable.Map[String, CurrentProcessing[MODEL_OUTPUT]]()

  override def preStart {
    // check first to see if there's anything to restore...
    if (filePersistence.stateExists(label)) {
      filePersistence.restoreMergerState(label) match {
        case Right(speculativeStreamMerger) ⇒
          timeout = speculativeStreamMerger.timeout
          required = speculativeStreamMerger.results
          log.info(s"Restored Speculative Stream Merger; timeout $timeout")
        case Left(unsuccessfulMessage) ⇒
          log.warning(unsuccessfulMessage)
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = {

    // Process a new merger definition
    case speculativeStreamMerger: SpeculativeStreamMerger ⇒
      log.info(s"Received new merger definition: ${speculativeStreamMerger}...")
      timeout = speculativeStreamMerger.timeout
      // persist new merger
      filePersistence.saveState(speculativeStreamMerger, label) match {
        case Left(error)  ⇒ log.error(error)
        case Right(true)  ⇒ log.info(s"Successfully saved state for merger $speculativeStreamMerger using location $label")
        case Right(false) ⇒ log.error(s"BUG: FilePersistence.saveState returned Right(false) for merger $speculativeStreamMerger.")
      }
      sender() ! Done

    // Start speculative requesr
    case start: StartSpeculative ⇒
      // Set up the state
      currentProcessing += (start.uuid -> CurrentProcessing(System.currentTimeMillis(), new ListBuffer[MODEL_OUTPUT]())) // Add to watch list
      sender() ! Done

    // New timer - check for expired requests
    case _: Long ⇒
      val startTime = System.currentTimeMillis() - timeout
      var submitted = false
      breakable {
        for ((uuid, processing) ← currentProcessing) {
          processing.start match {
            case start if (start < startTime) ⇒ // Timed out. To simplify, we only process 1 timeout at a time.
              processResult(uuid, processing)
              submitted = true
              break
            case _ ⇒
          }
        }
      }
      if (!submitted) {
        sender() != None
        ()
      }

    // Result of indivirual model serving
    case speculativerecord: SpecificRecordBase ⇒
      val uuid = recordsplitter.getUUID(speculativerecord)
      currentProcessing.get(uuid) match {
        case Some(processingResults) ⇒
          // We are still waiting for this GUID
          val current = CurrentProcessing(processingResults.start, processingResults.results += recordsplitter.getRecord(speculativerecord))
          current.results.size match {
            case size if (size >= required) ⇒
              processResult(uuid, current) // We are done
            case _ ⇒
              currentProcessing += (uuid -> current) // Keep going
              sender() != None
              ()
          }
        case _ ⇒ // It is already removed
          sender() != None
          ()
      }
  }

  // Complete speculative execution
  private def processResult(GUID: String, results: CurrentProcessing[MODEL_OUTPUT]): Unit = {
    sender() ! Some(decider.decideResult(results.results.toList))
    currentProcessing -= GUID // cleanup
  }
}

object SpeculativeModelServingCollectorActor {

  def props[MODEL_OUTPUT](
      label:          String,
      recordsplitter: SpeculativeRecordSplitter[MODEL_OUTPUT],
      decider:        Decider[MODEL_OUTPUT]): Props =
    Props(new SpeculativeModelServingCollectorActor[MODEL_OUTPUT](label, recordsplitter, decider))
}

case class CurrentProcessing[MODEL_OUTPUT](start: Long, results: ListBuffer[MODEL_OUTPUT])
