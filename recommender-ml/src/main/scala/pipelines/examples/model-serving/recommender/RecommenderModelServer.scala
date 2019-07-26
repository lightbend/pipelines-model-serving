package pipelines.examples.modelserving.recommender

import pipelines.examples.modelserving.recommender.data.{ ProductPrediction, RecommenderRecord, RecommenderResult }
import pipelines.examples.modelserving.recommender.models.tensorflow.RecommenderTensorFlowServingModelFactory
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType, ServingResult }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

import akka.Done
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.{ FlowWithPipelinesContext, RunnableGraphStreamletLogic }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import scala.concurrent.duration._

final case object RecommenderModelServer extends AkkaStreamlet {

  val dtype = "recommender"
  val in0 = AvroInlet[RecommenderRecord]("in-0")
  val in1 = AvroInlet[ModelDescriptor]("in-1")
  val out = AvroOutlet[RecommenderResult]("out", _.name)
  final override val shape = StreamletShape.withInlets(in0, in1).withOutlets(out)

  override final def createLogic = new RunnableGraphStreamletLogic() {

    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelserver = context.system.actorOf(
      ModelServingActor.props[RecommenderRecord, Seq[ProductPrediction]](
        "recommender", RecommenderTensorFlowServingModelFactory))

    def runnableGraph() = {
      atLeastOnceSource(in1).via(modelFlow).runWith(Sink.ignore)
      atLeastOnceSource(in0).via(dataFlow).to(atLeastOnceSink(out))
    }

    protected def dataFlow =
      FlowWithPipelinesContext[RecommenderRecord].mapAsync(1) {
        record ⇒
          modelserver.ask(record)
            .mapTo[ServingResult[Seq[ProductPrediction]]]
      }.filter {
        sr ⇒ sr.result != None // should only happen when there is no model for scoring.
      }.map {
        sr ⇒ RecommenderResult(sr.modelName, sr.duration, sr.result.get)
      }

    protected def modelFlow =
      FlowWithPipelinesContext[ModelDescriptor].mapAsync(1) {
        descriptor ⇒ modelserver.ask(descriptor).mapTo[Done]
      }
  }
}

object RecommenderModelServerMain {
  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem("ModelServing")
    implicit val askTimeout: Timeout = Timeout(30.seconds)

    val modelserver = system.actorOf(
      ModelServingActor.props[RecommenderRecord, Seq[ProductPrediction]](
        "recommender", RecommenderTensorFlowServingModelFactory))

    val descriptor = new ModelDescriptor(
      name = "Tensorflow Model",
      description = "For model Serving",
      modelType = ModelType.TENSORFLOWSERVING,
      modelBytes = None,
      modelSourceLocation = Some("http://recommender1-service-kubeflow.lightshift.lightbend.com/v1/models/recommender1/versions/1:predict"))

    println(s"Sending descriptor ${descriptor.toRichString} to the scoring engine...")
    modelserver.ask(descriptor)
    val record = new RecommenderRecord(10L, Seq(1L, 2L, 3L, 4L))
    Thread.sleep(1000)
    val result = modelserver.ask(record).mapTo[ServingResult[Seq[ProductPrediction]]]
    Thread.sleep(1000)
    println(result)
  }
}
