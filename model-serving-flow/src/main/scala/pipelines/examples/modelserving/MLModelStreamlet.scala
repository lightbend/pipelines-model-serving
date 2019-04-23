package pipelines.examples.modelserving

import java.util.{ Date, UUID }

import com.lightbend.modelserving.model.{ Model, ModelToServe }
import com.lightbend.modelserving.winemodel.pmml.WinePMMLModel
import pipelines.akkastream.scaladsl.{ Splitter, SplitterLogic }
import pipelines.examples.data.Codecs._
import pipelines.examples.data._

class MLModelStreamlet extends Splitter[MlAction, Result, ModelUpdateConfirm] {
  override def createLogic = new SplitterLogic[MlAction, Result, ModelUpdateConfirm]() {

    val defaultModelDescriptor = ModelDescriptorUtil.getDefaultModel()
    var currentModel: Model[WineRecord, Double] = WinePMMLModel.create(ModelToServe.fromModelDescriptor(defaultModelDescriptor)).get
    var modelId = "Model_" + UUID.randomUUID()

    def flow = contextPropagatedFlow()
      .map { action ⇒
        {
          if (action.action.equals("SCORE_RECORD")) {
            val start = new Date().getTime
            val result = currentModel.score(action.record)
            val time: Long = new Date().getTime - start
            println("Model Result: " + result + " Time: " + time)
            Left(Result(modelId, "wine_record", time, result, new Date().getTime))
          } else {
            println("model is being update to:" + action.model.data)
            currentModel = WinePMMLModel.create(ModelToServe.fromModelDescriptor(action.model)).get
            modelId = "Model_" + UUID.randomUUID()

            Right(ModelUpdateConfirm(modelId, action.model.description, action.model.dataType,
              action.model.modeltype, action.model.data, action.model.location, new Date().getTime))
          }
        }
      }
  }
}
