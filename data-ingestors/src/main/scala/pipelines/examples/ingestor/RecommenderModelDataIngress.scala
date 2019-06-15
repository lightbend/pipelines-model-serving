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
import pipelines.examples.util.ConfigUtil
import pipelines.examples.util.ConfigUtil.implicits._
import scala.concurrent.duration._

/**
 * Ingress of model updates. In this case, every two minutes we load and
 * send downstream a model from the previously-trained models that are
 * found in the "datamodel" subproject.
 */
class RecommenderModelDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[ModelDescriptor]("out", _.name)
  final override val shape = StreamletShape.withOutlets(out)

  protected lazy val configUtil = ConfigUtil.default
  protected lazy val serverLocations =
    configUtil.getOrFail[Seq[String]]("recommenders.service-urls").toVector
  protected lazy val modelFrequencySeconds =
    configUtil.getOrElse[Int]("recommenders.model-frequency-seconds")(120).seconds

  var serverIndex: Int = 0 // will be between 0 and serverLocations.size-1

  val source: Source[ModelDescriptor, NotUsed] =
    Source.repeat(NotUsed)
      .map(_ ⇒ getModelDescriptor())
      .throttle(1, modelFrequencySeconds)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph = source.to(atMostOnceSink(out))
  }

  def getModelDescriptor(): ModelDescriptor = {

    val i = nextServerIndex()
    val location = serverLocations(i)
    new ModelDescriptor(
      name = "Tensorflow Model", description = "For model Serving",
      modeltype = ModelType.TENSORFLOWSERVING, modeldata = None,
      modeldatalocation = Some(location), dataType = "recommender")
  }

  def nextServerIndex(): Int = {
    val i = serverIndex
    // increment for next call
    serverIndex = (serverIndex + 1) % serverLocations.size
    i
  }
}

object RecommenderModelDataIngress {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("RecommenderModelDataIngress-Main")
    implicit val mat = ActorMaterializer()
    val ingress = new RecommenderModelDataIngress()
    println(s"frequency (seconds): ${ingress.modelFrequencySeconds}")
    println(s"server URLs:         ${ingress.serverLocations}")
    ingress.source.runWith(Sink.foreach(println))
  }
}
