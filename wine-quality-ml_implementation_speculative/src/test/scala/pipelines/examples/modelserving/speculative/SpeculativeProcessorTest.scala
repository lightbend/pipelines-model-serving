package pipelines.examples.modelserving.speculative

import akka.actor.{ ActorRef, ActorSystem }
import akka.util.Timeout
import akka.pattern.ask
import com.lightbend.modelserving.model.ModelResultMetadata
import com.lightbend.modelserving.model.actor.SpeculativeModelServingCollectorActor
import com.lightbend.modelserving.speculative.StartSpeculative
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.speculative.model.{ WineDecider, WineSpeculativeRecordSplitter }
import pipelines.examples.modelserving.winequality.data.{ WineRecord, WineResult }
import pipelines.examples.modelserving.winequality.result.ModelDoubleResult
import pipelines.examples.modelserving.winequality.speculative.WineResultRun

import scala.concurrent.duration._

class SpeculativeProcessorTest extends FlatSpec {

  implicit val system: ActorSystem = ActorSystem("ModelServing")
  implicit val executor = system.getDispatcher
  implicit val askTimeout = Timeout(30.seconds)

  val record = WineRecord("", .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0)

  "Sending time tick to empty actor" should "return None" in {
    createSpeculativeCollector().ask(1l).mapTo[Option[WineResult]].map(result ⇒
      assert(result.isEmpty)
    )
    // Wait for completion
    Thread.sleep(3000)
  }

  "Sending time tick to an actor after StartSpeculative" should "return None" in {
    val speculativeCollector = createSpeculativeCollector()
    speculativeCollector.ask(StartSpeculative("id"))
    // Wait for completion
    Thread.sleep(3000)

    speculativeCollector.ask(1l).mapTo[Option[WineResult]].map(result ⇒
      assert(result.isEmpty)
    )
    // Wait for completion
    Thread.sleep(3000)
  }

  "Sending two results" should "return result" in {
    val speculativeCollector = createSpeculativeCollector()
    speculativeCollector.ask(StartSpeculative("id"))
    // Wait for completion
    Thread.sleep(3000)

    // First run
    val run1 = WineResultRun("id", WineResult(record, ModelDoubleResult(0.0), ModelResultMetadata("", "T1", "model1", 0L, 4L)))
    speculativeCollector.ask(run1).mapTo[Option[WineResult]].map(result ⇒
      assert(result.isEmpty)
    )
    // Wait for completion
    Thread.sleep(10)
    // Second run
    val run2 = WineResultRun("id", WineResult(record, ModelDoubleResult(0.0), ModelResultMetadata("", "T2", "model2", 0L, 4L)))
    speculativeCollector.ask(run2).mapTo[Option[WineResult]].map(result ⇒
      assert(result.isDefined)
    )

    // Wait for completion
    Thread.sleep(3000)
  }

  "Sending time tick with delay after the first result" should "return result" in {
    val speculativeCollector = createSpeculativeCollector()
    speculativeCollector.ask(StartSpeculative("id"))
    // Wait for completion
    Thread.sleep(3000)

    // First run
    val run1 = WineResultRun("id", WineResult(record, ModelDoubleResult(0.0), ModelResultMetadata("", "T1", "model1", 0L, 4L)))
    speculativeCollector.ask(run1).mapTo[Option[WineResult]].map(result ⇒
      assert(result.isEmpty)
    )
    // Wait for completion
    Thread.sleep(1010)
    // Tick
    speculativeCollector.ask(1l).mapTo[Option[WineResult]].map(result ⇒
      assert(result.isDefined)
    )

    // Wait for completion
    Thread.sleep(3000)
  }

  private def createSpeculativeCollector(): ActorRef = {

    val splitter = new WineSpeculativeRecordSplitter()
    val decider = new WineDecider()

    val speculativeCollector = system.actorOf(
      SpeculativeModelServingCollectorActor.props[WineResult](
        "speculativecollector",
        splitter,
        decider))

    // Wait for the actor to initialize and restore
    Thread.sleep(3000)
    speculativeCollector
  }
}
