package pipelines.examples.modelserving.bluegreen

import akka._
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.duration._
import pipelines.streamlets._
import pipelines.akkastream._
import pipelines.akkastream.scaladsl._
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import com.lightbend.modelserving.model.actor.{ DataSplittingActor, RecordWithOutlet }
import com.lightbend.modelserving.model.persistence.FilePersistence
import com.lightbend.modelserving.splitter.StreamSplitter
import pipelinesx.ingress.InputTrafficSplitter
import pipelines.examples.modelserving.winequality.data.WineRecord

final case object ModelSplitter extends AkkaStreamlet {

  val in0 = AvroInlet[WineRecord]("in-0")
  val in1 = AvroInlet[StreamSplitter]("in-1")
  val out0 = AvroOutlet[WineRecord]("out-0", _.lot_id)
  val out1 = AvroOutlet[WineRecord]("out-1", _.lot_id)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out0, out1)

  // Declare the volume mount: 
  private val persistentDataMount =
    VolumeMount("persistence-data-mount", "/data", ReadWriteMany)
  override def volumeMounts = Vector(persistentDataMount)
  FilePersistence.setGlobalMountPoint(persistentDataMount.path)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    FilePersistence.setStreamletName(context.streamletRef)
    val datasplitter = context.system.actorOf(
      DataSplittingActor.props("splitter"))

    def runnableGraph() = {

      val outlet0 = atLeastOnceSink(out0)
      val outlet1 = atLeastOnceSink(out1)

      atLeastOnceSource(in1).via(configFlow).to(atLeastOnceSink)
      val dt = atLeastOnceSource(in0).via(dataFlow)
      new InputTrafficSplitter[WineRecord](dt, outlet0, outlet1) {}.runnableGraph()
    }

    protected def dataFlow() =
      FlowWithPipelinesContext[WineRecord].mapAsync(1) { record ⇒
        datasplitter.ask(RecordWithOutlet(0, record)).mapTo[RecordWithOutlet].
          map(result ⇒ {
            result.outlet match {
              case i@_ if (i == 1) ⇒ Right(result.record.asInstanceOf[WineRecord])
              case _               ⇒ Left(result.record.asInstanceOf[WineRecord])
            }
          })
      }

    protected def configFlow =
      FlowWithPipelinesContext[StreamSplitter].mapAsync(1) {
        descriptor ⇒ datasplitter.ask(descriptor).mapTo[Done]
      }
  }
}
