package pipelines.examples.modelserving.winequality

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.streamlets.avro.AvroOutlet
import pipelines.streamlets.StreamletShape
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ RunnableGraphStreamletLogic }
import pipelinesx.config.ConfigUtil
import pipelinesx.config.ConfigUtil.implicits._
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

/**
 * One at a time every two minutes, loads a PMML or TensorFlow model and
 * sends it downstream.
 */
final case object WineModelIngress extends AkkaStreamlet {

  val out = AvroOutlet[ModelDescriptor]("out", _.modelType.toString)

  final override val shape = StreamletShape(out)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph =
      WineModelIngressUtil.makeSource().to(atMostOnceSink(out))
  }
}

object WineModelIngressUtil {

  lazy val modelFrequencySeconds: FiniteDuration =
    ConfigUtil.default
      .getOrElse[Int]("wine-quality.model-frequency-seconds")(120).seconds

  // TODO: Add this logic to ConfigUtil?.
  lazy val wineModelsResources: Map[ModelType, Seq[String]] =
    ConfigUtil.defaultConfig
      .getObject("wine-quality.model-sources").entrySet.asScala.foldLeft(
        Map.empty[ModelType, Seq[String]]) {
          case (map, e) ⇒
            val modelType = ModelType.valueOf(e.getKey.toUpperCase)
            val list = e.getValue.valueType.toString match {
              case "LIST"   ⇒ e.getValue.unwrapped.asInstanceOf[java.util.ArrayList[String]].toArray.map(_.toString)
              case "STRING" ⇒ Array(e.getValue.unwrapped.toString)
            }
            map + (modelType -> list)
        }

  def makeSource(
      modelsResources: Map[ModelType, Seq[String]] = wineModelsResources,
      frequency:       FiniteDuration              = modelFrequencySeconds): Source[ModelDescriptor, NotUsed] = {
    val recordsReader = WineModelReader(modelsResources)
    Source.repeat(recordsReader)
      .map(reader ⇒ reader.next())
      .throttle(1, frequency)
  }
}

/**
 * Test program for [[WineModelIngress]] and [[WineModelIngressUtil]].
 * It reads models and prints their data.
 */
object WineModelIngressMain {

  /**
   * For testing purposes.
   * At this time, Pipelines intercepts calls to sbt run and sbt runMain, so use
   * the console instead:
   * ```
   * import pipelines.examples.modelserving.winequality._
   * WineModelIngressMain.main(Array("-n", "5"))
   * ```
   */
  def main(args: Array[String]): Unit = {
    val defaultN = 100
    val defaultF = WineModelIngressUtil.dataFrequencyMilliseconds
    def help() = println(s"""
      |usage: WineModelIngressMain [-h|--help] [-n|--count N] [-f|--frequency F]
      |where:
      |  -h | --help         print this message and exit
      |  -n | --count N      print N records and stop (default: $defaultN)
      |  -f | --frequency F  seconds between output model descriptions (default: $defaultF)
      |""".stripMargin)

    def parseArgs(args2: Seq[String], nf: (Int,Int)): (Int,Int) = args2 match {
      case ("-h" | "--help") +: _ ⇒
        help()
        sys.exit(0)
      case Nil                                 ⇒ nf
      case ("-n" | "--count") +: n +: tail ⇒ parseArgs(tail, (n.toInt, nf._2))
      case ("-f" | "--frequency") +: n +: tail ⇒ parseArgs(tail, (nf._1, n.toInt.seconds))
      case x +: _ ⇒
        println(s"ERROR: Unrecognized argument $x. All args: ${args.mkString(" ")}")
        help()
        sys.exit(1)
    }

    val (count, frequency) = parseArgs(args, (defaultN, defaultF))

    println(s"printing $count records")
    println(s"frequency (seconds): $frequency")
    implicit val system = ActorSystem("WineModelIngressMain")
    implicit val mat = ActorMaterializer()
    val source = WineModelIngressUtil.makeSource(
      wineModelsResources.wineModelsResources, frequency)
    source.runWith {
      Sink.foreach { descriptor ⇒
        println(descriptor.toRichString)
        count -= 1
        if (count == 0) {
          println("Finished!")
          sys.exit(0)
        }
      }
    }
    println("Should never get here...")
  }
}
