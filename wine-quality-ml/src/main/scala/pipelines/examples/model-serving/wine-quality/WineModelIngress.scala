package pipelines.examples.modelserving.winequality

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import net.ceedubs.ficus.Ficus._
import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.duration._
import pipelinesx.modelserving.model.{ ModelDescriptor, ModelType }
import pipelinesx.modelserving.model.util.ModelMainBase

/**
 * One at a time every two minutes, loads a PMML or TensorFlow model and
 * sends it downstream.
 */
final case object WineModelIngress extends AkkaStreamlet {

  val out = AvroOutlet[ModelDescriptor]("out", _.modelType.toString)

  final override val shape = StreamletShape(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph =
      WineModelIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

object WineModelIngressUtil {

  private val config: Config = ConfigFactory.load()

  lazy val modelFrequencySeconds: FiniteDuration =
    config.as[Option[Int]]("wine-quality.model-frequency-seconds").getOrElse(120).seconds

  // Ficus can't quite infer the Map with the ModelType key, so we use strings, ...
  lazy val wineModelsResources: Map[ModelType, Seq[String]] =
    config.as[Map[String, Seq[String]]]("wine-quality.model-sources")
      .map { case (key, value) ⇒ (ModelType.valueOf(key.toUpperCase), value) }

  def makeSource(
      modelsResources: Map[ModelType, Seq[String]] = wineModelsResources,
      frequency:       FiniteDuration              = modelFrequencySeconds): Source[ModelDescriptor, NotUsed] = {
    val recordsReader = WineModelReader(modelsResources)
    Source.repeat(recordsReader)
      .map(reader ⇒ reader.next())
      .throttle(1, frequency)
  }
}

/**
 * Test program for [[WineModelIngress]] and [[WineModelIngressUtil]].
 * It reads models and prints their data. For testing purposes only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.winequality._
 * WineModelIngressMain.main(Array("-n","5","-f","1000"))
 * ```
 */
object WineModelIngressMain extends ModelMainBase(
  defaultCount = 5,
  defaultFrequencyMillis = WineModelIngressUtil.modelFrequencySeconds * 1000) {

  override protected def makeSource(frequency: FiniteDuration): Source[ModelDescriptor, NotUsed] =
    WineModelIngressUtil.makeSource(
      WineModelIngressUtil.wineModelsResources, frequency)
}
