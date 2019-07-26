package pipelines.examples.modelserving.winequality

import data.{ WineRecord, WineResult }
import models.pmml.WinePMMLModelFactory
import models.tensorflow.{ WineTensorFlowModelFactory, WineTensorFlowBundledModelFactory }

import akka.Done
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await

import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType, MultiModelFactory, ServingResult }
import com.lightbend.modelserving.model.util.MainBase

final case object WineModelServer extends AkkaStreamlet {

  val in0 = AvroInlet[WineRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[WineResult]("out", _.name)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  val modelFactory = MultiModelFactory(
    Map(
      ModelType.PMML -> WinePMMLModelFactory,
      ModelType.TENSORFLOW -> WineTensorFlowModelFactory,
      ModelType.TENSORFLOWSAVED -> WineTensorFlowBundledModelFactory))

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelserver = context.system.actorOf(
      ModelServingActor.props[WineRecord, Double]("wine", modelFactory))

    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }

    protected def dataFlow =
      FlowWithPipelinesContext[WineRecord].mapAsync(1) {
        data ⇒ modelserver.ask(data).mapTo[ServingResult[Double]]
      }.filter {
        sr ⇒ sr.result != None // should only happen when there is no model for scoring.
      }.map {
        sr ⇒ WineResult(sr.modelName, sr.duration, sr.result.get)
      }

    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelserver.ask(descriptor).mapTo[Done]
      }
  }
}

/**
 * Test program for [[WineModelServer]]. Just loads the PMML model and uses it
 * to score one record. So, this program focuses on ensuring the logic works
 * for any model, but doesn't exercise all the available models.
 * For testing purposes, only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.winequality._
 * WineModelServerMain.main(Array("-n","3","-f","1000"))
 * ```
 */
object WineModelServerMain {
  val defaultCount = 3
  val defaultFrequencyMillis = 1000.milliseconds

  def main(args: Array[String]): Unit = {
    val (count, frequency) =
      MainBase.parseArgs(args, this.getClass.getName, defaultCount, defaultFrequencyMillis)

    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelserver = system.actorOf(
      ModelServingActor.props[WineRecord, Double]("wine", WineModelServer.modelFactory))

    val path = "wine/models/winequalityDecisionTreeClassification.pmml"
    val is = this.getClass.getClassLoader.getResourceAsStream(path)
    val pmml = new Array[Byte](is.available)
    is.read(pmml)
    val descriptor = new ModelDescriptor(
      name = "Wine Model",
      description = "winequalityDecisionTreeClassification",
      modelType = ModelType.PMML,
      modelBytes = Some(pmml),
      modelSourceLocation = None)

    val record = WineRecord(
      "wine quality sample data",
      .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0)

    for (i ← 0 until count) {
      modelserver.ask(descriptor)
      Thread.sleep(100)
      val result = Await.result(modelserver.ask(record).mapTo[ServingResult[Double]], 5 seconds)
      println(s"$i: result - $result")
      Thread.sleep(frequency.length)
    }
    sys.exit(0)
  }
}
