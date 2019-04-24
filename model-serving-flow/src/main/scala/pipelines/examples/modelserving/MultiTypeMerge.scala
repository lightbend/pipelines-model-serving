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
        //        println("model is being update to:" + e.data)
        currentModel = WinePMMLModel.create(ModelToServe.fromModelDescriptor(model)).get
        modelId = "Model_" + UUID.randomUUID()

        //        Right(ModelUpdateConfirm(modelId, e.description, e.dataType,
        //          e.modeltype, e.data, e.location, new Date().getTime))

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

//final class FanInML[A,B,O](val combine: (A, B) ⇒ O) extends ZipWith2(combine.apply) {
//  override def toString = "Zip"
//}

//final class FanInML[In0: KeyedSchema, In1: KeyedSchema, Out: KeyedSchema] extends FanIn[Out] {
//  val inlet0 = KeyedInletPort[In0](FanIn.inletName(0))
//  val inlet1 = KeyedInletPort[In1](FanIn.inletName(1))
//
//  final override def inlets = Vector(inlet0, inlet1)
//}

//
//
//package pipelines.examples.streamlet
//
//import akka.NotUsed
//import akka.stream.ClosedShape
//import akka.stream.contrib.PartitionWith
//import akka.stream.javadsl.ZipWith
//import akka.stream.scaladsl.{GraphDSL, Keep, Merge, RunnableGraph, Zip, ZipWith2}
//import pipelines.akkastream.scaladsl.{ContextPropagatedFlow, FanInN, MergeLogic, RunnableGraphLogic, Splitter}
//import pipelines.akkastream.{AkkaStreamlet, PipelinesContext, StreamletContext}
//import pipelines.examples.data._
//import pipelines.examples.data.Codecs._
//import pipelines.streamlets._
//
//abstract class MultiTypeMerge[In0:KeyedSchema, In1:KeyedSchema, out: KeyedSchema] extends Splitter {
//  override implicit val shape = FanIn[In0, In1, out]
//
//  override final def createLogic: MultiTypeMergeLogic[In0, In1, out] = new MultiTypeMergeLogic()
//}
//
//class MultiTypeMergeLogic[In0, In1, out]()(implicit shape: FanIn2[In0, In1, out], context: StreamletContext) extends RunnableGraphLogic {
//
//  final def contextPropagatedFlow() = ContextPropagatedFlow[Either[In0, In1]]
//
//  override def runnableGraph() = {
//
//    val in0 = atLeastOnceSource[In0](shape.inlet0)
//    val in1 = atLeastOnceSource[In1](shape.inlet1)
//    val out = atLeastOnceSink[out](shape.outlet)
//
//    RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] ⇒
//      import GraphDSL.Implicits._
//      import pipelines.examples.streamlet.FanInML
//
//      val merge = builder.add(FanInML[In0, In1])
//
//      builder.add().addEdge(importAndGetPort(b), to)
//
//      ClosedShape
//    })
//  }
//}
//
////final class FanInML[A,B,O](val combine: (A, B) ⇒ O) extends ZipWith2(combine.apply) {
////  override def toString = "Zip"
////}
//
//object FanInML {
//  def apply[A, B](): Zip[A, B] = new Zip
//}
