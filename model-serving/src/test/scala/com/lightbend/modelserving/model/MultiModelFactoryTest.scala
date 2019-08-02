package com.lightbend.modelserving.model

import org.scalatest.FunSpec
import scala.concurrent.duration._

class MultiModelFactoryTest extends FunSpec {

  class TestModelFactory(whichOne: ModelType) extends ModelFactory[String, String] {
    protected def make(desc: ModelDescriptor): Either[String, Model[String, String]] =
      if (desc.modelType == whichOne) {
        val m = new Model[String, String] {
          val descriptor: ModelDescriptor = desc
          def score(record: String, stats: ModelServingStats): Model.ModelReturn[String] = {
            val mrm = ModelResultMetadata(
              modelType = ModelType.UNKNOWN.toString,
              modelName = "Unknown",
              errors = "",
              startTime = 0,
              duration = 1)
            Model.ModelReturn(record.length.toString, mrm, stats.incrementUsage(1.milliseconds))
          }
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

  def makeMF(): MultiModelFactory[String, String] = {
    val map = Map(
      ModelType.PMML -> testPMMLFactory,
      ModelType.TENSORFLOW -> testTensorFlowFactory,
      ModelType.H2O -> testH2OFactory)
    new MultiModelFactory[String, String](map)
  }

  def goodTest(): Unit = {
    val mmf = makeMF()
    val ds = Seq(pmmlDescriptor, tensorFlowDescriptor, h2oDescriptor)
    ds.foreach { descriptor ⇒
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
    }
  }
}
