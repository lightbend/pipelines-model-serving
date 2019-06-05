package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.modelserving.model.ModelCodecs._
import com.typesafe.config.ConfigFactory
import pipelines.akkastream.scaladsl._
import pipelines.examples.data._

import scala.concurrent.duration._
imoprt scala.collection.JavaConverters._

/**
  * Ingress of model updates. In this case, every two minutes we load and
  * send downstream a model from the previously-trained models that are
  * found in the "datamodel" subproject.
  */
class RecommenderModelDataIngress extends SourceIngress[ModelDescriptor] {

  protected lazy val config = ConfigFactory.load()

  protected lazy val serverLocations =
    config.getStringList("recommenders.urls").asScala.toVector

  var serverIndex: Int = 0 // will be between 0 and serverLocations.size-1

  override def createLogic = new SourceIngressLogic() {

    def source: Source[ModelDescriptor, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ getModelDescriptor())
        .throttle(1, 2.minutes) // "dribble" them out
    }
  }

  def getModelDescriptor(): ModelDescriptor = {

    val i = nextServerIndex()
    val location = serverLocations(i)
    new ModelDescriptor(
      name = "Tensorflow Model", description = "For model Serving",
      modeltype = ModelType.TENSORFLOWSERVING, modeldata = None,
      modeldatalocation = Some(location), dataType = "recommender")
  }

  def nextServerIndex(): Int = {
    val i = serverIndex
    // increment for next call
    serverIndex = (serverIndex + 1) % serverLocations.size
    i
  }
}

object RecommenderModelDataIngress {

  def main(args: Array[String]): Unit = {
    val ingress = new RecommenderModelDataIngress()
    while (true)
      println(ingress.getModelDescriptor())
  }
}
