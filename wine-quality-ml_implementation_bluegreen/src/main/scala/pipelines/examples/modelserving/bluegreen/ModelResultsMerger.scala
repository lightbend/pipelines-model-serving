package pipelines.examples.modelserving.bluegreen

import pipelines.streamlets._
import pipelines.akkastream._
import pipelines.akkastream.util.scaladsl.MergeLogic
import pipelines.streamlets.avro._
import pipelines.examples.modelserving.winequality.data.WineResult

class ModelResultsMerger extends AkkaStreamlet {

  val in0 = AvroInlet[WineResult]("in-0")
  val in1 = AvroInlet[WineResult]("in-1")
  val out = AvroOutlet[WineResult]("out", _.inputRecord.lot_id)

  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)
  override final def createLogic = new MergeLogic(Vector(in0, in1), out)
}
