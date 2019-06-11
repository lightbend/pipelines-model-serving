package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import com.lightbend.modelserving.model.ModelCodecs._
import pipelines.examples.data._

import scala.concurrent.duration._
import scala.collection.JavaConverters._

/**
 * One at a time every two minutes, loads a PMML or TensorFlow model and
 * sends it downstream.
 */
class WineModelDataIngress extends SourceIngress[ModelDescriptor] {

  protected lazy val modelFrequencySeconds =
    this.context.config.getInt("wine-quality.model-frequency-seconds")

  override def createLogic = new SourceIngressLogic() {

    val recordsReader =
      WineModelsReader(WineModelDataIngress.WineModelsResources)

    def source: Source[ModelDescriptor, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ recordsReader.next())
        .throttle(1, modelFrequencySeconds.seconds)
    }
  }
}

object WineModelDataIngress {

  protected lazy val config = com.typesafe.config.ConfigFactory.load()

  // TODO: Use one of the Scala wrappers for Typesafe Config that can do the
  // conversion to a Map more seamlessly.
  val WineModelsResources: Map[ModelType, Seq[String]] =
    config.getObject("wine-quality.model-sources").entrySet.asScala.foldLeft(
      Map.empty[ModelType, Seq[String]]) {
        case (map, e) ⇒
          val modelType = ModelType.valueOf(e.getKey.toUpperCase)
          val list = e.getValue.valueType.toString match {
            case "LIST"   ⇒ e.getValue.unwrapped.asInstanceOf[java.util.ArrayList[String]].toArray.map(_.toString)
            case "STRING" ⇒ Array(e.getValue.unwrapped.toString)
          }
          map + (modelType -> list)
      }
}
