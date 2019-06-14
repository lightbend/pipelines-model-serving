package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro._
import pipelines.akkastream.{ AkkaStreamlet, StreamletLogic }
import pipelines.examples.data._
import pipelines.examples.util.ConfigUtil
import pipelines.examples.util.ConfigUtil.implicits._
import scala.concurrent.duration._
import scala.collection.JavaConverters._

/**
 * One at a time every two minutes, loads a PMML or TensorFlow model and
 * sends it downstream.
 */
class WineModelDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[ModelDescriptor]("out", _.name)

  final override val shape = StreamletShape(out)

  val recordsReader =
    WineModelsReader(WineModelDataIngress.WineModelsResources)

  protected lazy val modelFrequencySeconds =
    ConfigUtil(this.context.config)
      .getOrElse[Int]("wine-quality.model-frequency-seconds")(120).seconds

  val source: Source[ModelDescriptor, NotUsed] =
    Source.repeat(NotUsed)
      .map(_ ⇒ recordsReader.next())
      .throttle(1, modelFrequencySeconds)

  override def createLogic = new StreamletLogic() {
    def run(): Unit = source.to(atLeastOnceSink(out))
  }
}

object WineModelDataIngress {

  protected lazy val config = com.typesafe.config.ConfigFactory.load()

  // TODO: Add this logic to ConfigUtil?.
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
