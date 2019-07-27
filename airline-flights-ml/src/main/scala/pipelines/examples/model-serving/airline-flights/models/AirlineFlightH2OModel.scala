package pipelines.examples.modelserving.airlineflights.models

import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult, ModelResult, ModelResultMetadata }
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelFactory, ModelType, ScoreMetadata }
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
  protected def initFrom(record: AirlineFlightRecord): AirlineFlightResult =
    AirlineFlightResult(
      modelResult = new ModelResult( // will be overwritten subsequently
        delayPredictionLabel = "",
        delayPredictionProbability = 0.0),
      modelResultMetadata = new ModelResultMetadata( // will be overwritten subsequently
        errors = "",
        modelType = ModelType.H2O.ordinal,
        modelName = "AirlineFlightH2OModel",
        duration = 0),
      year = record.year,
      month = record.month,
      dayOfMonth = record.dayOfMonth,
      dayOfWeek = record.dayOfWeek,
      depTime = record.depTime,
      crsDepTime = record.crsDepTime,
      arrTime = record.arrTime,
      crsArrTime = record.crsArrTime,
      uniqueCarrier = record.uniqueCarrier,
      flightNum = record.flightNum,
      tailNum = record.tailNum,
      actualElapsedTime = record.actualElapsedTime,
      crsElapsedTime = record.crsElapsedTime,
      airTime = record.airTime,
      arrDelay = record.arrDelay,
      depDelay = record.depDelay,
      origin = record.origin,
      destination = record.destination,
      distance = record.distance,
      taxiIn = record.taxiIn,
      taxiOut = record.taxiOut,
      canceled = record.canceled,
      cancellationCode = record.cancellationCode,
      diverted = record.diverted,
      carrierDelay = record.carrierDelay,
      weatherDelay = record.weatherDelay,
      nASDelay = record.nASDelay,
      securityDelay = record.securityDelay,
      lateAircraftDelay = record.lateAircraftDelay)

  protected def setScoreAndMetadata(
      out:      AirlineFlightResult,
      score:    Option[BinomialModelPrediction],
      metadata: ScoreMetadata): AirlineFlightResult = {
    val (probs, label) = score match {
      case Some(bmp) ⇒ (bmp.classProbabilities, bmp.label)
      case None      ⇒ (Array.empty[Double], "Unknown")
    }
    val probability = if (probs.length == 2) probs(1) else 0.0
    out.modelResult.delayPredictionLabel = label
    out.modelResult.delayPredictionProbability = probability
    out.modelResultMetadata.errors = metadata.errors
    out.modelResultMetadata.modelType = metadata.modelType.ordinal
    out.modelResultMetadata.modelName = metadata.modelName
    out.modelResultMetadata.duration = metadata.duration.length
    out
  }

  /** Score a record with the model */
  override protected def invokeModel(record: AirlineFlightRecord): (String, Option[BinomialModelPrediction]) = {
    val row = toRow(record)
    val prediction = h2oModel.predict(row).asInstanceOf[BinomialModelPrediction]
    ("", Some(prediction))
  }
}

/**
 * Factory for airline flight H2O model
 */
object AirlineFlightH2OModelFactory extends ModelFactory[AirlineFlightRecord, AirlineFlightResult] {

  protected def make(
      descriptor: ModelDescriptor): Either[String, Model[AirlineFlightRecord, AirlineFlightResult]] =
    if (descriptor == Model.noopModelDescriptor) Right(noopModel)
    else Right(new AirlineFlightH2OModel(descriptor))

  import hex.genmodel.easy.EasyPredictModelWrapper

  lazy val noopModel: Model[AirlineFlightRecord, AirlineFlightResult] =
    new AirlineFlightH2OModel(Model.noopModelDescriptor) with Model.NoopModel[AirlineFlightRecord, BinomialModelPrediction, AirlineFlightResult] {
      override protected def loadModel(descriptor: ModelDescriptor): EasyPredictModelWrapper = null
      override protected def invokeModel(record: AirlineFlightRecord): (String, Option[BinomialModelPrediction]) =
        noopInvokeModel(record)
    }
}
