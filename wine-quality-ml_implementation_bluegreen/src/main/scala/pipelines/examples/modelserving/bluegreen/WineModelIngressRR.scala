package pipelines.examples.modelserving.bluegreen

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.streamlets.avro.AvroOutlet
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelinesx.config.ConfigUtil
import pipelinesx.config.ConfigUtil.implicits._

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType }
import pipelines.examples.modelserving.winequality.WineModelReader
import pipelines.streamlets.StreamletShape
import pipelinesx.ingress.RoundRobinInputSplitter

/**
 * One at a time every two minutes, loads a PMML or TensorFlow model and
 * sends it downstream.
 */
final case object WineModelIngressRR extends AkkaStreamlet {

  val out0 = AvroOutlet[ModelDescriptor]("out-0", _.modelType.toString)
  val out1 = AvroOutlet[ModelDescriptor]("out-1", _.modelType.toString)

  final override val shape = StreamletShape.withOutlets(out0, out1)

  override def createLogic = new RunnableGraphStreamletLogic() {
    def runnableGraph = {
      val outlet0 = plainSink(out0)
      val outlet1 = plainSink(out1)
      new RoundRobinInputSplitter[ModelDescriptor](outlet0, outlet1) {
        def source = WineModelIngressRRUtil.makeSource()
      }.runnableGraph()
    }
  }
}

object WineModelIngressRRUtil {

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
