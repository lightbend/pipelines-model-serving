package pipelines.examples.modelserving.recommender

import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType, ModelFactory, ModelFactoryResolver }
import pipelines.examples.modelserving.recommender.data.{ ProductPrediction, RecommenderRecord }
import pipelines.examples.modelserving.recommender.models.tensorflow.RecommenderTensorFlowServingModel

/**
 * Model factory resolver - requires specific factories
 */
object RecommendationFactoryResolver extends ModelFactoryResolver[RecommenderRecord, Seq[ProductPrediction]] {

  private val factories = Map(
    ModelType.TENSORFLOWSERVING -> RecommenderTensorFlowServingModel)

  override def getFactory(descriptor: ModelDescriptor): Option[ModelFactory[RecommenderRecord, Seq[ProductPrediction]]] =
    factories.get(descriptor.modelType)
}
