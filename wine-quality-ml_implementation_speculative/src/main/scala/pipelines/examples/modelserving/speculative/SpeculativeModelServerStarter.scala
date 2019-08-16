package pipelines.examples.modelserving.speculative

import java.util.UUID

import akka.NotUsed
import akka.stream.ClosedShape
import akka.stream.scaladsl.{ GraphDSL, Keep, RunnableGraph }
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

      val outlet0 = atLeastOnceSink(out0)
      val outlet1 = atLeastOnceSink(out1)

      RunnableGraph.fromGraph(
        GraphDSL.create(outlet0, outlet1)(Keep.left) { implicit builder: GraphDSL.Builder[NotUsed] ⇒ (wine, start) ⇒
          import GraphDSL.Implicits._

          val guid = UUID.randomUUID().toString
          atLeastOnceSource(in).map(record ⇒ WineRecordRun(guid, record)) ~> wine
          atLeastOnceSource(in).map(_ ⇒ StartSpeculative(guid)) ~> start
          ClosedShape
        }
      )
    }
  }
}
