package pipelines.examples.ingestor

import java.io.BufferedInputStream

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import com.lightbend.modelserving.model.ModelCodecs._
import pipelines.examples.data._

import scala.concurrent.duration._

class ModelDataIngress extends SourceIngress[ModelDescriptor] {

  val PMMLnames: Seq[String] = Seq("/winequalityDecisionTreeClassification.pmml", "/winequalityDesisionTreeRegression.pmml",
    "/winequalityGeneralizedLinearRegressionGamma.pmml", "/winequalityGeneralizedLinearRegressionGaussian.pmml",
    "/winequalityLinearRegression.pmml", "/winequalityMultilayerPerceptron.pmml", "/winequalityRandonForrestClassification.pmml")
  val TFOptimized = "/optimized_WineQuality.pb"
  var PMML = false
  var PMMLIterator: Iterator[String] = _

  override def createLogic = new SourceIngressLogic() {

    def source: Source[ModelDescriptor, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ => getModelDescriptor())
        .throttle(1, 2.minutes) // "dribble" them out
    }
  }

  def getModelDescriptor(): ModelDescriptor = {

    PMML match {
      case false ⇒ // Tensorflow model
        PMML = true
        PMMLIterator = PMMLnames.iterator
        val bis = new BufferedInputStream(getClass.getResourceAsStream(TFOptimized))
        val barray = Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toArray
        bis.close()
        new ModelDescriptor(
          name = "Tensorflow Model",
          description = "generated from TensorFlow", modeltype = ModelType.TENSORFLOW, modeldata = Some(barray),
          modeldatalocation = None, dataType = "wine")
      case _ ⇒ // PMML Model
        PMMLIterator.hasNext match {
          case false ⇒ // Done with all PMML
            PMML = false
            getModelDescriptor()
          case _ ⇒ // Continue with PMML
            val f = PMMLIterator.next
            val bis = new BufferedInputStream(getClass.getResourceAsStream(f))
            val barray = Stream.continually(bis.read).takeWhile(-1 !=).map(_.toByte).toArray
            bis.close()
            new ModelDescriptor(
              name = f.dropRight(5),
              description = "generated from Spark", modeltype = ModelType.PMML, modeldata = Some(barray),
              modeldatalocation = None, dataType = "wine")
        }
    }
  }
}

object ModelDataIngress {
  def main(args: Array[String]): Unit = {
    val ingress = new ModelDataIngress()
    while (true)
      println(ingress.getModelDescriptor())
  }
}