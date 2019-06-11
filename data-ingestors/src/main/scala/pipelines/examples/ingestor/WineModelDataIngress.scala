package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import com.lightbend.modelserving.model.ModelCodecs._
import pipelines.examples.data._

import scala.concurrent.duration._

/**
 * One at a time every two minutes, loads a PMML or TensorFlow model and
 * sends it downstream.
 */
class WineModelDataIngress extends SourceIngress[ModelDescriptor] {

  override def createLogic = new SourceIngressLogic() {

    val recordsReader =
      WineModelsReader(WineModelDataIngress.WineModelsResources)

    def source: Source[ModelDescriptor, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ recordsReader.next())
        .throttle(1, 2.minutes) // "dribble" them out
    }
  }
}

object WineModelDataIngress {

  val WineModelsResources: Map[ModelType, Seq[String]] = Map(
    ModelType.PMML -> Array(
      "/winequalityDecisionTreeClassification.pmml",
      "/winequalityDesisionTreeRegression.pmml",
      "/winequalityGeneralizedLinearRegressionGamma.pmml",
      "/winequalityGeneralizedLinearRegressionGaussian.pmml",
      "/winequalityLinearRegression.pmml",
      "/winequalityMultilayerPerceptron.pmml",
      "/winequalityRandonForrestClassification.pmml"),
    ModelType.TENSORFLOW -> Array("/optimized_WineQuality.pb")
  )
}
