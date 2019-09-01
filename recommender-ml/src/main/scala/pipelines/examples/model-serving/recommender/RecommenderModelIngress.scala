package pipelines.examples.modelserving.recommender

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import net.ceedubs.ficus.Ficus._
import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.duration._
import pipelinesx.modelserving.model.{ ModelDescriptor, ModelType }
import pipelinesx.modelserving.model.util.ModelMainBase

/**
 * Ingress of model updates. In this case, every two minutes we load and
 * send downstream a model from the previously-trained models that are
 * found in the "datamodel" subproject.
 */
final case object RecommenderModelIngress extends AkkaStreamlet {

  val out = AvroOutlet[ModelDescriptor]("out", _.modelType.toString)

  final override val shape = StreamletShape.withOutlets(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph =
      RecommenderModelIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

/** Encapsulate the logic of iterating through the models ad infinitum. */
protected final class ModelDescriptorFinder(
    initialServerIndex: Int,
    serverLocations:    Vector[String]) {

  def getModelDescriptor(): ModelDescriptor = {
    val i = nextServerIndex()
    val location = serverLocations(i)
    new ModelDescriptor(
      modelType = ModelType.TENSORFLOWSERVING,
      modelName = "Recommender Model",
      description = "Recommender TF serving model Serving",
      modelBytes = None,
      modelSourceLocation = Some(location))
  }

  // will be between 0 and serverLocations.size-1
  protected var serverIndex: Int = initialServerIndex

  protected def nextServerIndex(): Int = {
    val i = serverIndex
    // increment for next call
    serverIndex =
      (serverIndex + 1) % serverLocations.size
    i
  }
}

object RecommenderModelIngressUtil {

  private val config: Config = ConfigFactory.load()

  lazy val recommenderServerLocations: Vector[String] =
    config.as[Option[Seq[String]]]("recommender.service-urls").getOrElse(Seq("http://tfserving.recommender-ml-boris.svc.cluster.local:8501/v1/models/recommender/versions/1:predict")).toVector
  lazy val modelFrequencySeconds: FiniteDuration =
    config.as[Option[Int]]("recommender.model-frequency-seconds").getOrElse(120).seconds

  /** Helper method extracted from RecommenderModelIngress for easier unit testing. */
  def makeSource(
      serverLocations: Vector[String] = recommenderServerLocations,
      frequency:       FiniteDuration = modelFrequencySeconds): Source[ModelDescriptor, NotUsed] = {
    val modelFinder = new ModelDescriptorFinder(0, serverLocations)
    Source.repeat(modelFinder)
      .map(finder ⇒ finder.getModelDescriptor())
      .throttle(1, frequency)
  }
}

/**
 * Test program for [[RecommenderModelIngress]] and [[RecommenderModelIngressUtil]].
 * It reads models and prints their data. For testing purposes only.
 * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
 * the console instead:
 * ```
 * import pipelines.examples.modelserving.recommender._
 * RecommenderModelIngressMain.main(Array("-n","3","-f","1000"))
 * ```
 */
object RecommenderModelIngressMain extends ModelMainBase(
  defaultCount = 3,
  defaultFrequencyMillis = RecommenderModelIngressUtil.modelFrequencySeconds * 1000) {

  override protected def makeSource(frequency: FiniteDuration): Source[ModelDescriptor, NotUsed] =
    RecommenderModelIngressUtil.makeSource(
      RecommenderModelIngressUtil.recommenderServerLocations, frequency)
}
