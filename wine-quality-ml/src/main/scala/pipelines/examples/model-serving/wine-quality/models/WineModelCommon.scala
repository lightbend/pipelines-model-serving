package pipelines.examples.modelserving.winequality

import data.{ WineRecord, WineResult, ModelResult, ModelResultMetadata }
import com.lightbend.modelserving.model.{ ModelImplTrait, ModelType, ScoreMetadata }

/**
 * Implements abstract [[Model]] methods that are the same for all the model types.
 */
trait WineModelCommon extends ModelImplTrait[WineRecord, Double, WineResult] {

  protected def initFrom(record: WineRecord): WineResult =
    new WineResult(
      modelResult = new ModelResult(quality = 0.0), // correct values filled in later.
      modelResultMetadata = new ModelResultMetadata( // correct values filled in later.
        errors = "",
        modelType = ModelType.UNKNOWN.ordinal,
        modelName = "",
        duration = 0),
      lot_id = record.lot_id,
      fixed_acidity = record.fixed_acidity,
      volatile_acidity = record.volatile_acidity,
      citric_acid = record.citric_acid,
      residual_sugar = record.residual_sugar,
      chlorides = record.chlorides,
      free_sulfur_dioxide = record.free_sulfur_dioxide,
      total_sulfur_dioxide = record.total_sulfur_dioxide,
      density = record.density,
      pH = record.pH,
      sulphates = record.sulphates,
      alcohol = record.alcohol)

  protected def setScoreAndMetadata(
      out:      WineResult,
      score:    Option[Double],
      metadata: ScoreMetadata): WineResult = {
    out.modelResult.quality = score.getOrElse(0.0)
    out.modelResultMetadata.errors = metadata.errors
    out.modelResultMetadata.modelType = metadata.modelType.ordinal
    out.modelResultMetadata.modelName = metadata.modelName
    out.modelResultMetadata.duration = metadata.duration.length
    out
  }
}
