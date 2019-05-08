package pipelines.examples.modelserving


import akka.NotUsed
import akka.stream.ClosedShape
import akka.stream.scaladsl.{ GraphDSL, RunnableGraph }
import com.lightbend.modelserving.model.{ Model, ModelToServe }
import pipelines.akkastream.scaladsl.RunnableGraphLogic
import pipelines.akkastream.{ AkkaStreamlet, StreamletContext }
import pipelines.examples.data._
import com.lightbend.modelserving.model.ModelCodecs._
import pipelines.examples.data.DataCodecs._
import pipelines.streamlets.{ FanIn, _ }

abstract class MultiTypeMerge extends AkkaStreamlet {
  override implicit val shape = new FanInOut[WineRecord, ModelDescriptor, Result]

  override final def createLogic: MultiTypeMergeLogic = new MultiTypeMergeLogic()
}

class MultiTypeMergeLogic()(implicit shape: FanInOut[WineRecord, ModelDescriptor, Result], context: StreamletContext) extends RunnableGraphLogic {

  //  final def contextPropagatedFlow() = ContextPropagatedFlow[Either[WineRecord, ModelDescriptor]]

  override def runnableGraph() = {

    val in0 = atLeastOnceSource[WineRecord](shape.inlet0)
    val in1 = atLeastOnceSource[ModelDescriptor](shape.inlet1)
    val out = atLeastOnceSink[Result](shape.outlet0)
    var currentModel: Option[Model[WineRecord, Double]] = None
    var currentModelName = ""

    RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] ⇒
      import GraphDSL.Implicits._

      in0.map(data => {
        currentModel match {
          case Some(model) ⇒ // There is model defined, process data
            val start = System.currentTimeMillis
            val result = model.score(data)
            val time = System.currentTimeMillis - start
            Result(currentModelName, data.dataType, time, Some(result))
          case _ ⇒ // Mo model available
            Result("No model", data.dataType, 0, None)
        }
      }) ~> out

      in1.map(model => {
        ModelToServe.toModel[WineRecord, Double](ModelToServe.fromModelRecord(model)) match {
          case Some(m) => // Successfully got a new model
            // close current model first
            currentModel.foreach(_.cleanup())
            // Update model and state
            currentModel = Some(m)
            currentModelName = model.name
           case _ =>   // Failed converting
            println(s"Failed to convert model: ${model.name}")
        }
      })
      ClosedShape
    })
  }
}

object FanInOut {
  val InletName = new IndexedPrefix("in", 9)
  val outletName = new IndexedPrefix("out", 9)
}

final class FanInOut[In0: KeyedSchema, In1: KeyedSchema, Out0: KeyedSchema] extends StreamletShape {
  val inlet0 = KeyedInletPort[In0](FanIn.inletName(0))
  val inlet1 = KeyedInletPort[In1](FanIn.inletName(1))

  val outlet0 = KeyedOutletPort[Out0](FanOut.outletName(0))

  final override def inlets = Vector(inlet0, inlet1)
  final override def outlets = Vector(outlet0)
}
