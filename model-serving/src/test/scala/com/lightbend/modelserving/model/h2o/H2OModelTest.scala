package com.lightbend.modelserving.model.h2o

import com.lightbend.modelserving.model.{ ModelDescriptor, ModelServingStats, ScoreMetadata }
import hex.genmodel.easy.RowData
import hex.genmodel.easy.prediction.BinomialModelPrediction
import pipelinesx.test.OutputInterceptor
import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import java.io.FileInputStream
import java.lang.{ Double ⇒ JDouble }

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
    row.put("delayPredictionProbability", new JDouble(probability))
    row
  }

  final class TestH2OModel(descriptor: ModelDescriptor)
    extends H2OModel[RowData, BinomialModelPrediction, RowData](descriptor) {

    override protected def invokeModel(record: RowData): (String, Option[BinomialModelPrediction]) = {
      val prediction = h2oModel.predict(record)
      ("", Some(prediction.asInstanceOf[BinomialModelPrediction]))
    }
    protected def initFrom(record: RowData): RowData = record // we just modify the input!
    protected def setScoreAndMetadata(
        out:      RowData,
        score:    Option[BinomialModelPrediction],
        metadata: ScoreMetadata): RowData = {

      score match {
        case Some(bmp) ⇒ toResult(out, bmp)
        case None      ⇒ fail("something failed!")
      }
      out.put("errors", metadata.errors)
      out.put("modelType", metadata.modelType)
      out.put("modelName", metadata.modelName)
      out.put("duration", metadata.duration)
      out
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
        val (servingResult, _) = testH2OModel.score(row1, ModelServingStats())
        assert("" == servingResult.get("errors").toString)
        val probability = servingResult.get("delayPredictionProbability").asInstanceOf[JDouble].doubleValue
        assert("YES" == servingResult.get("delayPredictionLabel").toString)
        assert(0.6 <= probability && probability <= 0.7)
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
