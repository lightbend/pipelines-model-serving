package pipelines.examples.modelserving.airlineflights.models

import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelFactory, ModelType }
import com.lightbend.modelserving.model.h2o.H2OModel
import hex.genmodel.easy.RowData
import hex.genmodel.easy.prediction.BinomialModelPrediction

class AirlineFlightH2OModel(descriptor: ModelDescriptor)
  extends H2OModel[AirlineFlightRecord, BinomialModelPrediction, AirlineFlightResult](descriptor) {

  val modelName = "AirlineFlightH2OModel"

  // Convert input record to raw data for serving
  def toRow(record: AirlineFlightRecord): RowData = {
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

  // create resulting message based on input and prediction result
  protected def makeOutRecord(
      record:    AirlineFlightRecord,
      errors:    String,
      score:     BinomialModelPrediction,
      duration:  Long,
      modelName: String,
      modelType: ModelType): AirlineFlightResult = {
    val probs = score.classProbabilities
    val probability = if (probs.length == 2) probs(1) else 0.0
    //    println(s"Prediction that flight departure will be delayed: ${score.label} (probability: $probability) for $record")
    val afr = AirlineFlightResult(
      errors = errors,
      delayPredictionLabel = score.label,
      delayPredictionProbability = probability,
      modelType = ModelType.H2O.toString,
      modelName = modelName,
      duration = duration,
      year = record.year,
      month = record.month,
      dayOfMonth = record.dayOfMonth,
      dayOfWeek = record.dayOfWeek,
      depTime = record.depTime,
      arrTime = record.arrTime,
      uniqueCarrier = record.uniqueCarrier,
      flightNum = record.flightNum,
      arrDelay = record.arrDelay,
      depDelay = record.depDelay,
      origin = record.origin,
      destination = record.destination)
    afr
  }

  /** Score a record with the model */
  protected def invokeModel(record: AirlineFlightRecord): (String, BinomialModelPrediction) = {
    val row = toRow(record)
    val prediction = h2oModel.predict(row)
    ("", prediction.asInstanceOf[BinomialModelPrediction])
  }
}

/**
 * Factory for airline flight H2O model
 */
object AirlineFlightH2OModelFactory extends ModelFactory[AirlineFlightRecord, AirlineFlightResult] {

  protected def make(descriptor: ModelDescriptor): Either[String, Model[AirlineFlightRecord, AirlineFlightResult]] =
    Right(new AirlineFlightH2OModel(descriptor))
}
