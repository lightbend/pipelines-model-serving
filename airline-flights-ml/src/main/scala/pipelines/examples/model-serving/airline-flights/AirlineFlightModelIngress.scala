package pipelines.examples.modelserving.airlineflights

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelinesx.config.ConfigUtil
import pipelinesx.config.ConfigUtil.implicits._
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
import scala.concurrent.duration._
import java.io.ByteArrayOutputStream

/**
 * Ingress of model updates. In this case, every two minutes we load and
 * send downstream a model. Because we have only one model we are resending it
 */
final case object AirlineFlightModelIngress extends AkkaStreamlet {

  // Use ONE partition for input to model serving
  val out = AvroOutlet[ModelDescriptor]("out", _ ⇒ "airlines")

  final override val shape = StreamletShape.withOutlets(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph =
      AirlineFlightModelIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

/** Encapsulate the logic of iterating through the models ad infinitum. */
protected final class ModelDescriptorProvider() {

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
      name = s"Airline flight Model $count (model #${index + 1})",
      description = "Airline H2O flight Model",
      modelType = ModelType.H2O,
      modelBytes = Some(sourceBytes(index)),
      modelSourceLocation = Some(sourcePaths(index)))
    println("AirlineFlightModelIngress: Returning " + md.toRichString)
    md
  }
}

object AirlineFlightModelIngressUtil {

  lazy val modelFrequencySeconds: FiniteDuration =
    ConfigUtil.default.getOrElse[Int](
      "airline-flights.model-frequency-seconds")(120).seconds
  lazy val modelSources: Seq[String] =
    ConfigUtil.default.getOrElse[Seq[String]](
      "airline-flights.model-sources.from-classpath.paths")(Nil)

  /** Helper method extracted from AirlineFlightModelIngress for easier unit testing. */
  def makeSource(
      frequency: FiniteDuration = modelFrequencySeconds): Source[ModelDescriptor, _] = {
    val provider = new ModelDescriptorProvider()
    Source.repeat(NotUsed)
      .map(_ ⇒ provider.getModelDescriptor())
      .throttle(1, frequency)
  }

  /**
   * For testing purposes.
   * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
   * the console instead:
   * ```
   * import pipelines.examples.modelserving.airlineflights._
   * AirlineFlightModelIngressUtil.main(Array("-f", "5"))
   * ```
   */
  def main(args: Array[String]): Unit = {
      def help() = println(s"""
      |usage: AirlineFlightModelIngressUtil [-h|--help] [-f|--frequency F]
      |where:
      |  -h | --help         print this message and exit
      |  -f | --frequency F  F seconds between output model descriptions (default: $modelFrequencySeconds)
      |""".stripMargin)

      def parseArgs(args2: Seq[String], freq: FiniteDuration): FiniteDuration = args2 match {
        case ("-h" | "--help") +: _ ⇒
          help()
          sys.exit(0)
        case Nil                                 ⇒ freq
        case ("-f" | "--frequency") +: n +: tail ⇒ parseArgs(tail, n.toInt.seconds)
        case x +: _ ⇒
          println(s"ERROR: Unrecognized argument $x. All args: ${args.mkString(" ")}")
          help()
          sys.exit(1)
      }

    val frequency = parseArgs(args, modelFrequencySeconds)
    println(s"Frequency (seconds): ${frequency}")
    implicit val system = ActorSystem("AirlineFlightModelIngress-Main")
    implicit val mat = ActorMaterializer()
    // val provider = new ModelDescriptorProvider()
    // println(s"Provider sourcePaths: ${provider.sourcePaths.mkString(", ")}")
    // println(s"Provider sourceBytes: ${provider.sourceBytes.take(64).mkString(" ")}")
    // println(s"next: ${provider.getModelDescriptor().toRichString}")
    // println(s"next: ${provider.getModelDescriptor().toRichString}")
    makeSource(frequency).runWith {
      Sink.foreach { md ⇒
        println(md.toRichString)
      }
    }.asInstanceOf[Unit] // silence warning about discarded value.
  }
}
