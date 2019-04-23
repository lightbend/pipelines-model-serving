package pipelines.examples.test

import com.lightbend.modelserving.model.{Model, ModelToServe}
import com.lightbend.modelserving.winemodel.pmml.WinePMMLModel
import pipelines.examples.data.{ModelDescriptorUtil, WineRecord}

object MLTest {

  val defaultModelDescriptor = ModelDescriptorUtil.getDefaultModel()
  var currentModel: Model[WineRecord, Double] = WinePMMLModel.create(ModelToServe.fromModelDescriptor(defaultModelDescriptor)).get

  def main(args: Array[String]): Unit = {
    var result = currentModel.score(WineRecord("Bla", 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0L))
    println(result)

    var result2 = currentModel.score(WineRecord("Bla", 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0L))
    println(result2)

    var result3 = currentModel.score(WineRecord("Bla", 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0L))
    println(result3)

    var result4 = currentModel.score(WineRecord("Bla", 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 0.0, 0L))
    println(result4)
  }
}
