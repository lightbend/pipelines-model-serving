package pipelines.examples.modelserving.winequality.models

import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType, ModelFactory, ModelFactoryResolver }
import pipelines.examples.modelserving.winequality.data.WineRecord
import pipelines.examples.modelserving.winequality.models.pmml.WinePMMLModel
import pipelines.examples.modelserving.winequality.models.tensorflow.{ WineTensorFlowBundledModel, WineTensorFlowModel }

/**
 * Model factory resolver - requires specific factories
 */
object WineFactoryResolver extends ModelFactoryResolver[WineRecord, Double] {

  private val factories = Map(
    ModelType.PMML -> WinePMMLModel,
    ModelType.TENSORFLOW -> WineTensorFlowModel,
    ModelType.TENSORFLOWSAVED -> WineTensorFlowBundledModel)

  override def getFactory(descriptor: ModelDescriptor): Option[ModelFactory[WineRecord, Double]] =
    factories.get(descriptor.modelType)
}
