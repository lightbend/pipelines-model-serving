package pipelines.examples.modelserving.winemodel

import com.lightbend.modelserving.model.{ ModelFactory, ModelFactoryResolver }
import pipelines.examples.data.{ ModelType, WineRecord }
import pipelines.examples.modelserving.winemodel.pmml.WinePMMLModel
import pipelines.examples.modelserving.winemodel.tensorflow.{ WineTensorFlowBundledModel, WineTensorFlowModel }

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
