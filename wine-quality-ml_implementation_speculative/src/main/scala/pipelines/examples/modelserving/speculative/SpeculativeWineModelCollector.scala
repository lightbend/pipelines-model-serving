package pipelines.examples.modelserving.speculative

import akka.{ Done, NotUsed }
import akka.pattern.ask
import akka.stream.scaladsl.{ Flow, Sink, Source }
import akka.util.Timeout
import com.lightbend.modelserving.model.actor.SpeculativeModelServingCollectorActor
import com.lightbend.modelserving.model.persistence.FilePersistence
import com.lightbend.modelserving.speculative.{ SpeculativeStreamMerger, StartSpeculative }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.examples.modelserving.speculative.model.{ WineDecider, WineSpeculativeRecordSplitter }
import pipelines.examples.modelserving.winequality.data.WineResult
import pipelines.examples.modelserving.winequality.speculative.WineResultRun
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import pipelines.streamlets.{ ReadWriteMany, StreamletShape, VolumeMount }

import scala.concurrent.duration._

final case object SpeculativeWineModelCollector extends AkkaStreamlet {

  val in0 = AvroInlet[WineResultRun]("in-0")
  val in1 = AvroInlet[SpeculativeStreamMerger]("in-1")
  val in2 = AvroInlet[StartSpeculative]("in-2")
  val out = AvroOutlet[WineResult]("out", _.inputRecord.lot_id)

  final override val shape = StreamletShape.withInlets(in0, in1, in2).withOutlets(out)

  // Declare the volume mount: 
  private val persistentDataMount =
    VolumeMount("persistence-data-mount", "/data", ReadWriteMany)
  override def volumeMounts = Vector(persistentDataMount)
  FilePersistence.setGlobalMountPoint(persistentDataMount.path)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)
    val splitter = new WineSpeculativeRecordSplitter()
    val decider = new WineDecider()

    FilePersistence.setStreamletName(context.streamletRef)
    val splieeterCollector = context.system.actorOf(
      SpeculativeModelServingCollectorActor.props[WineResult](
        "speculativecollector",
        splitter,
        decider))

    def runnableGraph() = {
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
      atLeastOnceSource(in1).via(configFlow).runWith(Sink.ignore)
      atLeastOnceSource(in2).via(startFlow).runWith(Sink.ignore)
      makeSource().via(timeFlow).to(atMostOnceSink(out))
    }

    protected def dataFlow =
      FlowWithPipelinesContext[WineResultRun].mapAsync(1) { record ⇒
        splieeterCollector.ask(record).mapTo[Option[WineResult]]
          .collect { case Some(result) ⇒ result }
      }

    protected def timeFlow =
      Flow[Long].mapAsync(1) { record ⇒
        splieeterCollector.ask(record).mapTo[Option[WineResult]]
          .collect { case Some(result) ⇒ result }
      }

    protected def configFlow =
      FlowWithPipelinesContext[SpeculativeStreamMerger].mapAsync(1) {
        descriptor ⇒ splieeterCollector.ask(descriptor).mapTo[Done]
      }

    protected def startFlow =
      FlowWithPipelinesContext[StartSpeculative].mapAsync(1) {
        descriptor ⇒ splieeterCollector.ask(descriptor).mapTo[Done]
      }

    def makeSource(frequency: FiniteDuration = 5.millisecond): Source[Long, NotUsed] =
      Source.repeat(1L)
        .throttle(1, frequency)
  }
}