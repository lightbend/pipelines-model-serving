package pipelines.examples.modelserving.airlineflightmodel

import com.lightbend.modelserving.model.{ Model, ModelFactory, ModelToServe }
import com.lightbend.modelserving.model.h2o.H2OModel
import hex.genmodel.easy.RowData
import hex.genmodel.easy.prediction.BinomialModelPrediction
import pipelines.examples.data.{ AirlineFlightRecord, AirlineFlightResult }

class AirlineFlightH2OModel(inputStream: Array[Byte]) extends H2OModel[AirlineFlightRecord, AirlineFlightResult](inputStream) {

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
  def toResult(record: AirlineFlightRecord, prediction: BinomialModelPrediction): AirlineFlightResult = {
    val probs = prediction.classProbabilities
    val probability = if (probs.length == 2) probs(1) else 0.0
//    println(s"Prediction that flight departure will be delayed: ${prediction.label} (probability: $probability) for $record")
    AirlineFlightResult(
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
      destination = record.destination,
      delayPredictionLabel = prediction.label,
      delayPredictionProbability = probability,
      modelname = "",
      dataType =  "",
      duration = 0)
  }

  /** Score a record with the model */
  override def score(input: AirlineFlightRecord): AirlineFlightResult = {
    val row = toRow(input)
    val prediction = model.predict(row)
    toResult(input, prediction.asInstanceOf[BinomialModelPrediction])
  }
}

/**
 * Factory for airline flight H2O model
 */
object AirlineFlightH2OModel extends ModelFactory[AirlineFlightRecord, AirlineFlightResult] {

  override def create(input: ModelToServe): Option[Model[AirlineFlightRecord, AirlineFlightResult]] = {
    try {
      Some(new AirlineFlightH2OModel(input.model))
    } catch {
      case _: Throwable ⇒ None
    }
  }

  override def restore(bytes: Array[Byte]): Model[AirlineFlightRecord, AirlineFlightResult] = new AirlineFlightH2OModel(bytes)
}
