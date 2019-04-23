//package pipelines.examples.streamlet
//
//import pipelines.akkastream.AkkaStreamlet
//import pipelines.akkastream.scaladsl.Merge
//import pipelines.examples.data._
//import pipelines.examples.data.Codecs._
//import pipelines.streamlets.{KeyedSchema, SingleInletShape}
//
//abstract class ConsoleEgress[In: KeyedSchema] extends Merge {
//  override implicit val shape = SingleInletShape[In]
//  // TODO override createLogic
//}