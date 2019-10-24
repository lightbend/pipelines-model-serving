package pipelines.examples.modelserving.recommender

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelinesx.config.ConfigUtil
import pipelinesx.config.ConfigUtil.implicits._
import scala.concurrent.duration._
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType }

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
      RecommenderModelIngressUtil.makeSource().to(plainSink(out))
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

  lazy val recommenderServerLocations: Vector[String] =
    ConfigUtil.default.getOrElse[Seq[String]]("recommender.service-urls")(Seq("http://tfserving.recommender-ml-boris.svc.cluster.local:8501/v1/models/recommender/versions/1:predict")).toVector
  lazy val modelFrequencySeconds: FiniteDuration =
    ConfigUtil.default.getOrElse[Int]("recommender.model-frequency-seconds")(120).seconds

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
