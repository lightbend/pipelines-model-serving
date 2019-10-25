package pipelines.examples.modelserving.speculative

import akka.{ Done, NotUsed }
import akka.pattern.ask
import akka.stream.scaladsl.{ Flow, Source }
import akka.util.Timeout
import com.lightbend.modelserving.model.actor.SpeculativeModelServingCollectorActor
import com.lightbend.modelserving.model.persistence.FilePersistence
import com.lightbend.modelserving.speculative.{ SpeculativeStreamMerger, StartSpeculative }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithOffsetContext, RunnableGraphStreamletLogic }
import pipelines.examples.modelserving.speculative.model.{ WineDecider, WineSpeculativeRecordSplitter }
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import pipelines.streamlets.{ ReadWriteMany, StreamletShape, VolumeMount }
import pipelines.examples.modelserving.winequality.data.WineResult
import pipelines.examples.modelserving.winequality.speculative.WineResultRun

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

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)
    val splitter = new WineSpeculativeRecordSplitter()
    val decider = new WineDecider()

    // Set persistence
    FilePersistence.setGlobalMountPoint(getMountedPath(persistentDataMount).toString)
    FilePersistence.setStreamletName(streamletRef)

    val splieeterCollector = context.system.actorOf(
      SpeculativeModelServingCollectorActor.props[WineResult](
        "speculativecollector",
        splitter,
        decider))

    def runnableGraph() = {
      sourceWithOffsetContext(in1).via(configFlow).runWith(sinkWithOffsetContext)
      sourceWithOffsetContext(in2).via(startFlow).runWith(sinkWithOffsetContext)
      plainSource(in0).via(dataFlow).merge(makeSource().via(timeFlow)).to(plainSink(out))
    }

    protected def dataFlow =
      Flow[WineResultRun].mapAsync(1) { record ⇒
        splieeterCollector.ask(record).mapTo[Option[WineResult]]
      }.collect { case Some(result) ⇒ result }

    protected def timeFlow =
      Flow[Long].mapAsync(1) { record ⇒
        splieeterCollector.ask(record).mapTo[Option[WineResult]]
      }.collect { case Some(result) ⇒ result }

    protected def configFlow =
      FlowWithOffsetContext[SpeculativeStreamMerger].mapAsync(1) {
        descriptor ⇒ splieeterCollector.ask(descriptor).mapTo[Done]
      }

    protected def startFlow =
      FlowWithOffsetContext[StartSpeculative].mapAsync(1) {
        descriptor ⇒ splieeterCollector.ask(descriptor).mapTo[Done]
      }

    def makeSource(frequency: FiniteDuration = 5.millisecond): Source[Long, NotUsed] =
      Source.repeat(1L)
        .throttle(1, frequency)
  }
}
