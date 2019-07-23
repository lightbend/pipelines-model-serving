package com.lightbend.modelserving.model.h2o

import com.lightbend.modelserving.model.ModelDescriptor
import hex.genmodel.easy.RowData
import hex.genmodel.easy.prediction.BinomialModelPrediction
import pipelinesx.test.OutputInterceptor
import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import java.io.FileInputStream

// TODO: Uses the Airline model as an example, but needs to be made more generic.
class H2OModelTest extends FunSpec with BeforeAndAfterAll with OutputInterceptor {

  override def afterAll: Unit = {
    resetOutputs()
  }

  val row1 = toRow("1990", "1", "3", "3", "1707", "US", "ORD", "IAD")

  // Convert input record to raw data for serving
  def toRow(
      year:          String,
      month:         String,
      dayofMonth:    String, // note spelling...
      dayOfWeek:     String,
      crsDepTime:    String,
      uniqueCarrier: String,
      origin:        String,
      dest:          String): RowData = {
    val row = new RowData
    row.put("Year", year)
    row.put("Month", month)
    row.put("DayofMonth", dayofMonth)
    row.put("DayOfWeek", dayOfWeek)
    row.put("CRSDepTime", crsDepTime)
    row.put("UniqueCarrier", uniqueCarrier)
    row.put("Origin", origin)
    row.put("Dest", dest)
    row
  }

  // Modifies the input!
  def toResult(row: RowData, prediction: BinomialModelPrediction): RowData = {
    val probs = prediction.classProbabilities
    val probability = if (probs.length == 2) probs(1) else 0.0
    row.put("delayPredictionLabel", prediction.label)
    row.put("delayPredictionProbability", probability.toString)
    row
  }

  final class TestH2OModel(descriptor: ModelDescriptor)
    extends H2OModel[RowData, RowData](descriptor) {
    /** Score a record with the model */
    override def score(input: RowData): Either[String, RowData] = {
      val prediction = model.predict(input)
      Right(toResult(input, prediction.asInstanceOf[BinomialModelPrediction]))
    }
  }

  val modelPath = "model-serving/src/test/resources/airlines/models/mojo/gbm_pojo_test.zip"

  describe("H2OModel") {
    it("loads a model from a zip archive contained in the descriptor.modelBytes") {
      ignoreOutput {
        val fis = new FileInputStream(modelPath)
        val available = fis.available
        val buffer = Array.fill[Byte](available)(0)
        val numBytes = fis.read(buffer)
        assert(numBytes == available)
        val descriptor = H2OModel.defaultDescriptor
          .copy(modelBytes = Some(buffer), modelSourceLocation = Some(modelPath))
        val testH2OModel = new TestH2OModel(descriptor)
        testH2OModel.score(row1) match {
          case Left(error) ⇒ fail(error)
          case Right(result) ⇒
            assert("YES" == result.get("delayPredictionLabel"))
            val prob = result.get("delayPredictionProbability").toString.toDouble
            assert(0.60 < prob && prob < 0.65)
        }
      }
    }

    it("raises an exception if the model can't be loaded from the descriptor.modelBytes") {
      intercept[AssertionError] {
        ignoreOutput {
          new TestH2OModel(H2OModel.defaultDescriptor)
        }
      }
    }
  }
}
