package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelines.examples.data._
import pipelines.examples.util.ConfigUtil
import pipelines.examples.util.ConfigUtil.implicits._
import scala.concurrent.duration._

// import scala.concurrent.duration._

/**
 * Ingress of model updates. In this case, every two minutes we load and
 * send downstream a model from the previously-trained models that are
 * found in the "datamodel" subproject.
 */
class RecommenderModelDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[ModelDescriptor]("out", _.name)
  final override val shape = StreamletShape.withOutlets(out)

  protected lazy val configUtil = ConfigUtil(this.context.config)
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
    // def runnableGraph = source.to(atLeastOnceSink(out))
    def runnableGraph = source.via(flow).to(atLeastOnceSink(out))
    def flow = FlowWithPipelinesContext[ModelDescriptor].map(identity)
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
    val ingress = new RecommenderModelDataIngress()
    while (true)
      println(ingress.getModelDescriptor())
  }
}
