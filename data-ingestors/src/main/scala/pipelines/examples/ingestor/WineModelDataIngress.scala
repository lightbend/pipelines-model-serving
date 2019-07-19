package pipelines.examples.ingestor

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.examples.data._
import pipelines.config.ConfigUtil
import pipelines.config.ConfigUtil.implicits._
import scala.concurrent.duration._
import scala.collection.JavaConverters._

/**
 * One at a time every two minutes, loads a PMML or TensorFlow model and
 * sends it downstream.
 */
final case object WineModelDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[ModelDescriptor]("out", _.dataType)

  final override val shape = StreamletShape(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph =
      WineModelDataIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

object WineModelDataIngressUtil {

  lazy val modelFrequencySeconds: FiniteDuration =
    ConfigUtil.default
      .getOrElse[Int]("wine-quality.model-frequency-seconds")(120).seconds

  // TODO: Add this logic to ConfigUtil?.
  lazy val wineModelsResources: Map[ModelType, Seq[String]] =
    ConfigUtil.defaultConfig
      .getObject("wine-quality.model-sources").entrySet.asScala.foldLeft(
        Map.empty[ModelType, Seq[String]]) {
          case (map, e) ⇒
            val modelType = ModelType.valueOf(e.getKey.toUpperCase)
            val list = e.getValue.valueType.toString match {
              case "LIST"   ⇒ e.getValue.unwrapped.asInstanceOf[java.util.ArrayList[String]].toArray.map(_.toString)
              case "STRING" ⇒ Array(e.getValue.unwrapped.toString)
            }
            map + (modelType -> list)
        }

  def makeSource(
      modelsResources: Map[ModelType, Seq[String]] = wineModelsResources,
      frequency: FiniteDuration = modelFrequencySeconds): Source[ModelDescriptor, NotUsed] = {
    val recordsReader = WineModelsReader(modelsResources)
    Source.repeat(recordsReader)
      .map(reader ⇒ reader.next())
      .throttle(1, frequency)
  }

  /** For testing purposes. */
  def main(args: Array[String]): Unit = {
    println(s"frequency (seconds): ${modelFrequencySeconds}")
    println(s"records sources:     ${wineModelsResources}")
    implicit val system = ActorSystem("WineModelDataIngress-Main")
    implicit val mat = ActorMaterializer()
    val source = makeSource(wineModelsResources, modelFrequencySeconds)
    source.runWith(Sink.foreach(println))
  }
}
