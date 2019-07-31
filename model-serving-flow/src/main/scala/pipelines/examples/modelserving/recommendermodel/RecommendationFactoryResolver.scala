package pipelines.examples.modelserving.recommendermodel

import com.lightbend.modelserving.model.{ ModelFactory, ModelFactoryResolver }
import pipelines.examples.data.{ ModelType, ProductPrediction, RecommenderRecord }
import pipelines.examples.modelserving.recommendermodel.tensorflow.RecommenderTensorflowServingModel

/**
 * Model factory resolver - requires specific factories
 */
object RecommendationFactoryResolver extends ModelFactoryResolver[RecommenderRecord, Seq[ProductPrediction]] {

  private val factories = Map(
    ModelType.TENSORFLOWSERVING.ordinal -> RecommenderTensorflowServingModel)

  override def getFactory(whichFactory: Int): Option[ModelFactory[RecommenderRecord, Seq[ProductPrediction]]] = factories.get(whichFactory)
}