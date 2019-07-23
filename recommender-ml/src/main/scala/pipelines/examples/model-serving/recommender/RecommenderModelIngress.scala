package pipelines.examples.modelserving.recommender

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
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

  /** For testing purposes. */
  def main(args: Array[String]): Unit = {
      def help() = println(s"""
      |usage: RecommenderModelIngressUtil [-h|--help] [N]
      |where:
      |  -h | --help       print this message and exit
      |  N                 N seconds between output model descriptions (default: $modelFrequencySeconds)
      |""".stripMargin)

      def parseArgs(args2: Seq[String], freq: FiniteDuration): FiniteDuration = args2 match {
        case ("-h" | "--help") +: _ ⇒
          help()
          sys.exit(0)
        case Nil       ⇒ freq
        case n +: tail ⇒ parseArgs(tail, n.toInt.seconds)
        case x +: _ ⇒
          println(s"ERROR: Unrecognized argument $x. All args: ${args.mkString(" ")}")
          help()
          sys.exit(1)
      }
    val frequency = parseArgs(args, modelFrequencySeconds)
    println(s"frequency (seconds): ${frequency}")
    println(s"server URLs:         ${recommenderServerLocations}")
    implicit val system = ActorSystem("RecommenderModelIngress-Main")
    implicit val mat = ActorMaterializer()
    val source = makeSource(recommenderServerLocations, frequency)
    source.runWith(Sink.foreach(println))
    println("Finished!")
  }
}
