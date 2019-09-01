package pipelines.examples.modelserving.airlineflights.models

import pipelines.examples.modelserving.airlineflights.data.AirlineFlightRecord
import pipelinesx.modelserving.model.{ Model, ModelDescriptor, ModelFactory }
import pipelinesx.modelserving.model.h2o.H2OModel
import hex.genmodel.easy.RowData
import hex.genmodel.easy.prediction.BinomialModelPrediction

class AirlineFlightH2OModel(descriptor: ModelDescriptor)
  extends H2OModel[AirlineFlightRecord, BinomialModelPrediction](
    descriptor)(() ⇒ new BinomialModelPrediction()) {
  println(s"Creating AirlineFlightH2OModel ${descriptor.description}")

  /** Convert input record to raw data for serving. */
  protected def toRow(record: AirlineFlightRecord): RowData = {
    val row = new RowData
    row.put("Year", record.year.toString)
    row.put("Month", record.month.toString)
    row.put("DayofMonth", record.dayOfMonth.toString) // note spelling...
    row.put("DayOfWeek", record.dayOfWeek.toString)
    row.put("CRSDepTime", record.crsDepTime.toString) // spelling...
    row.put("UniqueCarrier", record.uniqueCarrier.toString)
    row.put("Origin", record.origin.toString)
    row.put("Dest", record.destination.toString) // spelling...
    row
  }

  /** Score a record with the model */
  override protected def invokeModel(input: AirlineFlightRecord): Either[String, BinomialModelPrediction] = {
    val row = toRow(input)
    try {
      val prediction = h2oModel.predict(row).asInstanceOf[BinomialModelPrediction]
      Right(prediction)
    } catch {
      case t: Throwable ⇒ Left(t.getMessage)
    }
  }
}

/**
 * Factory for airline flight H2O model
 */
object AirlineFlightH2OModelFactory extends ModelFactory[AirlineFlightRecord, BinomialModelPrediction] {

  protected def make(descriptor: ModelDescriptor): Either[String, Model[AirlineFlightRecord, BinomialModelPrediction]] =
    Right(new AirlineFlightH2OModel(descriptor))
}
