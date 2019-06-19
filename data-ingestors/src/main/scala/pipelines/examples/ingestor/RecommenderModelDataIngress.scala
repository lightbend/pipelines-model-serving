package pipelines.examples.ingestor

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelines.examples.data._
import pipelines.util.ConfigUtil
import pipelines.util.ConfigUtil.implicits._
import scala.concurrent.duration._

/**
 * Ingress of model updates. In this case, every two minutes we load and
 * send downstream a model from the previously-trained models that are
 * found in the "datamodel" subproject.
 */
class RecommenderModelDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[ModelDescriptor]("out", _.name)

  final override val shape = StreamletShape.withOutlets(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph = source.to(atMostOnceSink(out))
  }

  protected def source(): Source[ModelDescriptor, NotUsed] = {
    val modelFinder = new ModelDescriptorFinder(0)
    Source.repeat(modelFinder)
      .map(finder ⇒ finder.getModelDescriptor())
      .throttle(1, RecommenderModelDataIngress.modelFrequencySeconds)
  }
}

/** Encapsulate the logic of iterating through the models ad infinitum. */
protected final class ModelDescriptorFinder(initialServerIndex: Int) {

  def getModelDescriptor(): ModelDescriptor = {
    val i = nextServerIndex()
    val location = RecommenderModelDataIngress.recommenderServerLocations(i)
    new ModelDescriptor(
      name = "Tensorflow Model", description = "For model Serving",
      modeltype = ModelType.TENSORFLOWSERVING, modeldata = None,
      modeldatalocation = Some(location), dataType = "recommender")
  }

  // will be between 0 and recommenderServerLocations.size-1
  protected var serverIndex: Int = initialServerIndex

  protected def nextServerIndex(): Int = {
    val i = serverIndex
    // increment for next call
    serverIndex =
      (serverIndex + 1) % RecommenderModelDataIngress.recommenderServerLocations.size
    i
  }
}

object RecommenderModelDataIngress {

  lazy val recommenderServerLocations: Vector[String] =
    ConfigUtil.default.getOrFail[Seq[String]]("recommenders.service-urls").toVector
  lazy val modelFrequencySeconds: FiniteDuration =
    ConfigUtil.default.getOrElse[Int]("recommenders.model-frequency-seconds")(120).seconds

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("RecommenderModelDataIngress-Main")
    implicit val mat = ActorMaterializer()
    val ingress = new RecommenderModelDataIngress()
    println(s"frequency (seconds): ${modelFrequencySeconds}")
    println(s"server URLs:         ${recommenderServerLocations}")
    ingress.source().runWith(Sink.foreach(println))
  }
}
