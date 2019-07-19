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
import pipelines.config.ConfigUtil
import pipelines.config.ConfigUtil.implicits._
import scala.concurrent.duration._

/**
 * Ingress of model updates. In this case, every two minutes we load and
 * send downstream a model. Because we have only one model we are resending it
 */
final case object AirlineFlightModelDataIngress extends AkkaStreamlet {

  val out = AvroOutlet[ModelDescriptor]("out", _.dataType)

  final override val shape = StreamletShape.withOutlets(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph =
      AirlineFlightModelDataIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

/** Encapsulate the logic of iterating through the models ad infinitum. */
protected final class ModelDescriptorProvider() {

  val is = this.getClass.getClassLoader.getResourceAsStream("airlines/models/mojo/gbm_pojo_test.zip")
  val mojo = new Array[Byte](is.available)
  is.read(mojo)
  var index = -1l

  def getModelDescriptor(): ModelDescriptor = {
    index = index + 1
    new ModelDescriptor(
      name = s"Airline flight Model $index", description = "Airline H2O flight Model",
      modeltype = ModelType.H2O, modeldata = Some(mojo),
      modeldatalocation = None, dataType = "airline")
  }
}

object AirlineFlightModelDataIngressUtil {

  lazy val modelFrequencySeconds: FiniteDuration =
    ConfigUtil.default.getOrElse[Int]("airlineflight.model-frequency-seconds")(120).seconds

  /** Helper method extracted from AirlineFlightModelDataIngress for easier unit testing. */
  def makeSource(
      frequency: FiniteDuration = modelFrequencySeconds): Source[ModelDescriptor, NotUsed] = {
    val modelFinder = new ModelDescriptorProvider()
    Source.repeat(modelFinder)
      .map(finder ⇒ finder.getModelDescriptor())
      .throttle(1, frequency)
  }

  /** For testing purposes. */
  def main(args: Array[String]): Unit = {
    def help() = println(s"""
      |usage: AirlineFlightModelDataIngressUtil [-h|--help] [N]
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
    implicit val system = ActorSystem("AirlineFlightModelDataIngress-Main")
    implicit val mat = ActorMaterializer()
    val source = makeSource(frequency)
    source.runWith(Sink.foreach(println))
  }
}
