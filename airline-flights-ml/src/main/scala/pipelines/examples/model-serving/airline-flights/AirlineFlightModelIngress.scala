package pipelines.examples.modelserving.airlineflights

import akka.NotUsed
import akka.stream.scaladsl.Source
import scala.concurrent.duration._
import java.io.ByteArrayOutputStream

import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import net.ceedubs.ficus.Ficus._
import com.typesafe.config.{ Config, ConfigFactory }

import pipelinesx.modelserving.model.{ ModelDescriptor, ModelType }
import pipelinesx.modelserving.model.ModelDescriptorUtil.implicits._
import pipelinesx.modelserving.model.util.ModelMainBase

/**
 * Ingress of model updates. In this case, every two minutes we load and
 * send downstream a model. Because we have only one model we are resending it
 */
final case object AirlineFlightModelIngress extends AkkaStreamlet {

  // Use ONE partition for input to model serving
  val out = AvroOutlet[ModelDescriptor]("out", _ ⇒ "airlines")

  final override val shape = StreamletShape.withOutlets(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph = AirlineFlightModelIngressUtil
      .makeSource(errorLogger = system.log.error _)
      .to(atMostOnceSink(out))
  }
}

/** Encapsulate the logic of iterating through the models ad infinitum. */
protected final class ModelDescriptorProvider(
    errorLogger: String ⇒ Unit = println) {

  val sourcePaths: Array[String] =
    AirlineFlightModelIngressUtil.modelSources.toArray
  val sourceBytes: Array[Array[Byte]] = sourcePaths map { path ⇒
    val is = this.getClass.getClassLoader.getResourceAsStream(path)
    val buffer = new Array[Byte](1024)
    val content = new ByteArrayOutputStream()
    Stream.continually(is.read(buffer)).takeWhile(_ != -1).foreach(content.write(buffer, 0, _))
    val mojo = content.toByteArray
    mojo
  }

  var count = -1

  def getModelDescriptor(): ModelDescriptor = {
    count += 1
    val index = count % sourceBytes.length
    val md = new ModelDescriptor(
      modelType = ModelType.H2O,
      modelName = s"Airline flight Model $count (model #${index + 1})",
      description = "Airline H2O flight Model",
      modelBytes = Some(sourceBytes(index)),
      modelSourceLocation = Some(sourcePaths(index)))
    println("AirlineFlightModelIngress: Returning " + md.toRichString)
    md
  }
}

object AirlineFlightModelIngressUtil {

  private val config: Config = ConfigFactory.load()

  lazy val modelFrequencySeconds: FiniteDuration =
    config.as[Option[Int]](
      "airline-flights.model-frequency-seconds").getOrElse(120).seconds
  lazy val modelSources: Seq[String] =
    config.as[Option[Seq[String]]](
      "airline-flights.model-sources.from-classpath.paths").getOrElse(Nil)

  /** Helper method extracted from AirlineFlightModelIngress for easier unit testing. */
  def makeSource(
      frequency:   FiniteDuration = modelFrequencySeconds,
      errorLogger: String ⇒ Unit  = println): Source[ModelDescriptor, NotUsed] = {
    val provider = new ModelDescriptorProvider(errorLogger)
    Source.repeat(NotUsed)
      .map(_ ⇒ provider.getModelDescriptor())
      .throttle(1, frequency)
  }
}

/**
 * Test program for [[AirlineFlightModelIngress]] and [[AirlineFlightModelIngressUtil]].
 * It reads models and prints their data. For testing purposes only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.airlineflights._
 * AirlineFlightModelIngressMain.main(Array("-n","20","-f","1000"))
 * ```
 */
object AirlineFlightModelIngressMain extends ModelMainBase(
  defaultCount = 20,
  defaultFrequencyMillis = AirlineFlightModelIngressUtil.modelFrequencySeconds * 1000) {

  override protected def makeSource(frequency: FiniteDuration): Source[ModelDescriptor, NotUsed] =
    AirlineFlightModelIngressUtil.makeSource(frequency)
}

