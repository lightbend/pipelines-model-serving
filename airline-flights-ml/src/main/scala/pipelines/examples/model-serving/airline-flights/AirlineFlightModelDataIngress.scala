package pipelines.examples.modelserving.airlineflights

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelinesx.config.ConfigUtil
import pipelinesx.config.ConfigUtil.implicits._
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
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

  val sourcePaths: Array[String] =
    AirlineFlightModelDataIngressUtil.modelSources.toArray
  val sourceBytes: Array[Array[Byte]] = sourcePaths map { path ⇒
    val is = this.getClass.getClassLoader.getResourceAsStream(path)
    val mojo = new Array[Byte](is.available)
    is.read(mojo)
    mojo
  }

  var count = -1

  def getModelDescriptor(): ModelDescriptor = {
    count += 1
    val index = count % sourceBytes.length
    new ModelDescriptor(
      name = s"Airline flight Model $count (model #${index + 1})",
      description = "Airline H2O flight Model",
      dataType = "airline",
      modelType = ModelType.H2O,
      modelBytes = Some(sourceBytes(index)),
      modelSourceLocation = Some(sourcePaths(index)))
  }
}

object AirlineFlightModelDataIngressUtil {

  lazy val modelFrequencySeconds: FiniteDuration =
    ConfigUtil.default.getOrElse[Int](
      "airline-flights.model-frequency-seconds")(120).seconds
  lazy val modelSources: Seq[String] =
    ConfigUtil.default.getOrElse[Seq[String]](
      "airline-flights.model-sources.from-classpath.paths")(Nil)

  /** Helper method extracted from AirlineFlightModelDataIngress for easier unit testing. */
  def makeSource(
      frequency: FiniteDuration = modelFrequencySeconds): Source[ModelDescriptor, _] = {
    val provider = new ModelDescriptorProvider()
    Source.tick(0.seconds, frequency, provider)
      .map(p ⇒ p.getModelDescriptor())
  }

  /**
   * For testing purposes.
   * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
   * the console instead:
   * ```
   * > console
   * scala> import pipelines.examples.modelserving.airlineflights._
   * scala> AirlineFlightModelDataIngressUtil.main(Array("-f", "15", "-n", "3"))
   */
  def main(args: Array[String]): Unit = {
      def help() = println(s"""
      |usage: AirlineFlightModelDataIngressUtil [-h|--help] [-n|--count N] [-f|--frequency F]
      |where:
      |  -h | --help         print this message and exit
      |  -n | --count N      N total iterations before stopping (default: doesn't stop)
      |  -f | --frequency F  F seconds between output model descriptions (default: $modelFrequencySeconds)
      |""".stripMargin)

      def parseArgs(args2: Seq[String], opts: (Long, FiniteDuration)): (Long, FiniteDuration) = args2 match {
        case ("-h" | "--help") +: _ ⇒
          help()
          sys.exit(0)
        case Nil                                 ⇒ opts
        case ("-n" | "--count") +: n +: tail     ⇒ parseArgs(tail, (n.toLong, opts._2))
        case ("-f" | "--frequency") +: n +: tail ⇒ parseArgs(tail, (opts._1, n.toInt.seconds))
        case x +: _ ⇒
          println(s"ERROR: Unrecognized argument $x. All args: ${args.mkString(" ")}")
          help()
          sys.exit(1)
      }

    val (total, frequency) = parseArgs(args, (Long.MaxValue, modelFrequencySeconds))
    println(s"# of iterations: ${if (total < Long.MaxValue) total.toString else "infinite"}")
    println(s"Frequency (seconds): ${frequency}")
    implicit val system = ActorSystem("AirlineFlightModelDataIngress-Main")
    implicit val mat = ActorMaterializer()
    val provider = new ModelDescriptorProvider()
    println(s"Provider sourcePaths: ${provider.sourcePaths.mkString(", ")}")
    println(s"Provider sourceBytes: ${provider.sourceBytes.take(64).mkString(" ")}")
    println(s"next: ${provider.getModelDescriptor().toRichString}")
    println(s"next: ${provider.getModelDescriptor().toRichString}")
    makeSource(frequency).take(total).runWith(
      Sink.foreach(md ⇒ println(md.toRichString)))
    println("Finished!")
    println("\n\nCalling exit. If in sbt, ignore 'sbt.TrapExitSecurityException'...\n\n")
    sys.exit(0)
  }
}
