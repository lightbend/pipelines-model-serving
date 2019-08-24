package pipelines.examples.modelserving.speculative

import pipelines.akkastream._
import pipelines.akkastream.util.scaladsl.MergeLogic
import pipelines.examples.modelserving.winequality.speculative.WineResultRun
import pipelines.streamlets._
import pipelines.streamlets.avro._

class IntermediateModelResultsMerger extends AkkaStreamlet {

  val in0 = AvroInlet[WineResultRun]("in-0")
  val in1 = AvroInlet[WineResultRun]("in-1")
  val out = AvroOutlet[WineResultRun]("out", _.result.inputRecord.lot_id)

  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)
  override final def createLogic = new MergeLogic[WineResultRun](Vector(in0, in1), out)
}
