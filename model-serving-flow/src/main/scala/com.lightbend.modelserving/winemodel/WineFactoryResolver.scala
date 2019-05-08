package com.lightbend.modelserving.winemodel

import com.lightbend.modelserving.model.{ModelFactory, ModelFactoryResolver}
import com.lightbend.modelserving.winemodel.pmml.WinePMMLModel
import com.lightbend.modelserving.winemodel.tensorflow.{WineTensorFlowBundledModel, WineTensorFlowModel}
import pipelines.examples.data.{ModelType, WineRecord}

/**
  * Model factory resolver - requires specific factories
  */
object WineFactoryResolver extends ModelFactoryResolver[WineRecord, Double]{

  private val factories = Map(ModelType.PMML.ordinal -> WinePMMLModel,
    ModelType.TENSORFLOW.ordinal -> WineTensorFlowModel,
    ModelType.TENSORFLOWSAVED.ordinal -> WineTensorFlowBundledModel)

  override def getFactory(whichFactory: Int): Option[ModelFactory[WineRecord, Double]] = factories.get(whichFactory)
}
