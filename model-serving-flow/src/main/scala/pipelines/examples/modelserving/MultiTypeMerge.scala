package pipelines.examples.modelserving

import java.util.{ Date, UUID }

import akka.NotUsed
import akka.stream.{ ClosedShape, FlowShape }
import akka.stream.scaladsl.{ GraphDSL, RunnableGraph }
import com.lightbend.modelserving.model.{ Model, ModelToServe }
import com.lightbend.modelserving.winemodel.pmml.WinePMMLModel
import pipelines.akkastream.scaladsl.{ ContextPropagatedFlow, FanInN, MergeLogic, RunnableGraphLogic, Splitter }
import pipelines.akkastream.{ AkkaStreamlet, PipelinesContext, StreamletContext }
import pipelines.examples.data._
import pipelines.examples.data.Codecs._
import pipelines.streamlets.{ FanIn, _ }

abstract class MultiTypeMerge extends AkkaStreamlet {
  override implicit val shape = new FanInOut[WineRecord, ModelDescriptor, Result, ModelUpdateConfirm]

  override final def createLogic: MultiTypeMergeLogic = new MultiTypeMergeLogic()
}

class MultiTypeMergeLogic()(implicit shape: FanInOut[WineRecord, ModelDescriptor, Result, ModelUpdateConfirm], context: StreamletContext) extends RunnableGraphLogic {

  final def contextPropagatedFlow() = ContextPropagatedFlow[Either[WineRecord, ModelDescriptor]]

  val defaultModelDescriptor = ModelDescriptorUtil.getDefaultModel()
  var currentModel: Model[WineRecord, Double] = WinePMMLModel.create(ModelToServe.fromModelDescriptor(defaultModelDescriptor)).get
  var modelId = "Model_" + UUID.randomUUID()

  override def runnableGraph() = {

    val in0 = atLeastOnceSource[WineRecord](shape.inlet0)
    val in1 = atLeastOnceSource[ModelDescriptor](shape.inlet1)
    val out0 = atLeastOnceSink[Result](shape.outlet0)
    val out1 = atLeastOnceSink[ModelUpdateConfirm](shape.outlet1)

    RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] ⇒
      import GraphDSL.Implicits._

      in0.map(e ⇒ {
        val start = new Date().getTime
        val result = currentModel.score(e)
        val time: Long = new Date().getTime - start
        //        println("Model Result: " + result + " Time: " + time)

        Result(modelId, "wine_record", time, result, new Date().getTime)
      }) ~> out0

      in1.map(model ⇒ {
        currentModel = WinePMMLModel.create(ModelToServe.fromModelDescriptor(model)).get
        modelId = "Model_" + UUID.randomUUID()

        ModelUpdateConfirm(modelId, model.description, model.dataType,
          model.modeltype, model.data, model.location, new Date().getTime)
      }) ~> out1

      ClosedShape
    })
  }
}

object FanInOut {
  val InletName = new IndexedPrefix("in", 9)
  val outletName = new IndexedPrefix("out", 9)
}

final class FanInOut[In0: KeyedSchema, In1: KeyedSchema, Out0: KeyedSchema, Out1: KeyedSchema] extends StreamletShape {
  val inlet0 = KeyedInletPort[In0](FanIn.inletName(0))
  val inlet1 = KeyedInletPort[In1](FanIn.inletName(1))

  val outlet0 = KeyedOutletPort[Out0](FanOut.outletName(0))
  val outlet1 = KeyedOutletPort[Out1](FanOut.outletName(1))

  final override def inlets = Vector(inlet0, inlet1)
  final override def outlets = Vector(outlet0, outlet1)
}