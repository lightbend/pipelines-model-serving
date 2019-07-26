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
import com.lightbend.modelserving.model.util.ModelMainBase

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
      name = "Tensorflow Model",
      description = "For model Serving",
      modelType = ModelType.TENSORFLOWSERVING,
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
    ConfigUtil.default.getOrFail[Seq[String]]("recommender.service-urls").toVector
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
