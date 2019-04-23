package pipelines.examples.data

object ModelDescriptorUtil {

  def getModelValue(modelType: String): Int = {
    var modelValue: Int = 0

    if (modelType.equals("TENSORFLOW")) {
      modelValue = 0
    } else if (modelType.equals("TENSORFLOWSAVED")) {
      modelValue = 1
    } else if (modelType.equals("PMML")) {
      modelValue = 2
    }

    return modelValue
  }

  def getDefaultModel(): ModelDescriptor = {
    val model = scala.io.Source.fromResource("winequalityMultilayerPerceptron.pmml").getLines().mkString

    ModelDescriptor("winequalityLinearRegression", "Linear Regression", "WineRecord", "PMML", model, null)
  }
}
