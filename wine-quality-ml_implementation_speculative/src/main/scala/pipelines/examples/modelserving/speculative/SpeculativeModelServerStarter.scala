package pipelines.examples.modelserving.speculative

import java.util.UUID

import com.lightbend.modelserving.speculative.StartSpeculative
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.examples.modelserving.winequality.data.WineRecord
import pipelines.examples.modelserving.winequality.speculative.WineRecordRun
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }

final case object SpeculativeModelServerStarter extends AkkaStreamlet {

  val in = AvroInlet[WineRecord]("in")
  val out0 = AvroOutlet[WineRecordRun]("out-0", _.uuid)
  val out1 = AvroOutlet[StartSpeculative]("out-1", _.uuid)
  final override val shape = StreamletShape.withInlets(in).withOutlets(out0, out1)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    def runnableGraph() = {

      atMostOnceSource(in)
        .map(WineRecordRun(UUID.randomUUID().toString, _))
        .alsoTo(atMostOnceSink(out0))
        .map(r ⇒ StartSpeculative(r.uuid)).to(atMostOnceSink(out1))
    }
  }
}
