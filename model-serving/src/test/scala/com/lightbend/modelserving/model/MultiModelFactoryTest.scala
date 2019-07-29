package com.lightbend.modelserving.model

import org.scalatest.FunSpec

class MultiModelFactoryTest extends FunSpec {

  class TestModelFactory(whichOne: ModelType) extends ModelFactory[String, String] {
    protected def make(desc: ModelDescriptor): Either[String, Model[String, String]] =
      if (desc.modelType == whichOne) {
        val m = new Model[String, String] {
          val descriptor: ModelDescriptor = desc
          def score(record: String, stats: ModelServingStats): (String, ModelServingStats) =
            (record, stats)
        }
        Right(m)
      } else {
        Left(s"Expected model type $whichOne. Got ${desc.modelType}")
      }
  }

  val testPMMLFactory = new TestModelFactory(ModelType.PMML)
  val testTensorFlowFactory = new TestModelFactory(ModelType.TENSORFLOW)
  val testH2OFactory = new TestModelFactory(ModelType.H2O)
  val testUnknownFactory = new TestModelFactory(ModelType.UNKNOWN)

  val pmmlDescriptor = ModelDescriptorUtil.unknown.copy(modelType = ModelType.PMML)
  val tensorFlowDescriptor = ModelDescriptorUtil.unknown.copy(modelType = ModelType.TENSORFLOW)
  val tensorFlowServingDescriptor = ModelDescriptorUtil.unknown.copy(modelType = ModelType.TENSORFLOWSERVING)
  val h2oDescriptor = ModelDescriptorUtil.unknown.copy(modelType = ModelType.H2O)

  def makeMF(addUnknown: Boolean = false): MultiModelFactory[String, String] = {
    val map = Map(
      ModelType.PMML -> testPMMLFactory,
      ModelType.TENSORFLOW -> testTensorFlowFactory,
      ModelType.H2O -> testH2OFactory)
    val map2 =
      if (addUnknown) map + (ModelType.UNKNOWN -> testUnknownFactory)
      else map
    new MultiModelFactory[String, String](map2)
  }

  def goodTest(addUnknown: Boolean = false): Unit = {
    val mmf = makeMF(addUnknown)
    val ds = Seq(pmmlDescriptor, tensorFlowDescriptor, h2oDescriptor)
    val ds2 = if (addUnknown) ModelDescriptorUtil.unknown +: ds else ds
    ds2.foreach { descriptor ⇒
      mmf.create(descriptor) match {
        case Right(model) ⇒ assert(descriptor.modelType == model.descriptor.modelType)
        case Left(errors) ⇒ fail(errors)
      }
    }
  }

  describe("MultiModelFactory") {

    it("holds separate factories, one per specified ModelType") { goodTest() }

    describe("create") {

      it("picks the correct factory based on the ModelType") { goodTest() }

      it("returns a Left(error) if no factory matches the ModelType") {
        val mmf = makeMF()
        mmf.create(tensorFlowServingDescriptor) match {
          case Left(errors@_) ⇒ // okay
          case Right(model@_) ⇒ fail("Should have failed, but didn't!")
        }
      }

      it("to return a NoopModel when ModelType.UNKNOWN is specified, the map requires an entry for it") {
        goodTest(true)
        val mmf = makeMF(false)
        mmf.create(ModelDescriptorUtil.unknown) match {
          case Left(errors@_) ⇒ // okay
          case Right(model)   ⇒ fail(s"should not have returned a model: $model")
        }
      }
    }
  }
}
