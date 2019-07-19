package pipelines.examples.modelserving.winequality.models

import com.lightbend.modelserving.model.{ ModelType, ModelFactory, ModelFactoryResolver }
import pipelines.examples.modelserving.winequality.data.WineRecord
import pipelines.examples.modelserving.winequality.models.pmml.WinePMMLModel
import pipelines.examples.modelserving.winequality.models.tensorflow.{ WineTensorFlowBundledModel, WineTensorFlowModel }

/**
 * Model factory resolver - requires specific factories
 */
object WineFactoryResolver extends ModelFactoryResolver[WineRecord, Double] {

  private val factories = Map(
    ModelType.PMML.ordinal -> WinePMMLModel,
    ModelType.TENSORFLOW.ordinal -> WineTensorFlowModel,
    ModelType.TENSORFLOWSAVED.ordinal -> WineTensorFlowBundledModel)

  override def getFactory(whichFactory: Int): Option[ModelFactory[WineRecord, Double]] = factories.get(whichFactory)
}
