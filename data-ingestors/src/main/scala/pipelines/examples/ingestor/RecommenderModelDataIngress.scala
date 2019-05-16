package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.lightbend.modelserving.model.ModelCodecs._
import pipelines.akkastream.scaladsl._
import pipelines.examples.data._

import scala.concurrent.duration._

class RecommenderModelDataIngress extends SourceIngress[ModelDescriptor] {

  var server = 1

  override def createLogic = new SourceIngressLogic() {

    def source: Source[ModelDescriptor, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ getModelDescriptor())
        .throttle(1, 2.minutes) // "dribble" them out
    }
  }

  def getModelDescriptor(): ModelDescriptor = {

    val s = getserver()
    val location = s"http://recommender$s-service-kubeflow.lightshift.lightbend.com/v1/models/recommender$s/versions/1:predict"
    new ModelDescriptor(
      name = "Tensorflow Model", description = "For model Serving",
      modeltype = ModelType.TENSORFLOWSERVING, modeldata = None,
      modeldatalocation = Some(location), dataType = "recommender")
  }

  def getserver(): String = {
    server = server + 1
    if (server > 1) server = 0
    server match {
      case 0 ⇒ ""
      case _ ⇒ "1"
    }
  }
}

object RecommenderModelDataIngress {
  def main(args: Array[String]): Unit = {
    val ingress = new RecommenderModelDataIngress()
    while (true)
      println(ingress.getModelDescriptor())
  }
}
