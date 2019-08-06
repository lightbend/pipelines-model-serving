package pipelines.examples.modelserving.recommender

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.persistence.FilePersistence
import com.lightbend.modelserving.model.{Model, ModelDescriptor, ModelType}
import pipelines.examples.modelserving.recommender.data.RecommenderRecord
import pipelines.examples.modelserving.recommender.result.ModelKeyDoubleValueArrayResult
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.recommender.models.tensorflow.RecommenderTensorFlowServingModelFactory

import scala.concurrent.duration._

class RecommenderCompleteTest extends FlatSpec {

  implicit val system: ActorSystem = ActorSystem("ModelServing")
  implicit val executor = system.getDispatcher
  implicit val askTimeout = Timeout(30.seconds)

  val products =  Seq(1L, 2L, 3L, 4L)

  val input = new RecommenderRecord(10L,products)

  private def getModel(): ModelDescriptor = {
    new ModelDescriptor(
      modelName = "Recomendation model",
      description = "TF Serving recomendation model complete test",
      modelType = ModelType.TENSORFLOWSERVING,
      modelBytes = None,
      modelSourceLocation = Some("http://localhost:8501/v1/models/recommender/versions/1:predict"))
  }

  "Processing of H2OModel" should "return label yes and probability around 0.6" in {

    FilePersistence.setStreamletName("streamlet")
    val modelserver = system.actorOf(
      ModelServingActor.props[RecommenderRecord, ModelKeyDoubleValueArrayResult](
        "recommendor", RecommenderTensorFlowServingModelFactory, () ⇒ new ModelKeyDoubleValueArrayResult))
    // Wait for the actor to initialize and restore
    Thread.sleep(2000)

    modelserver.ask(getModel())
    // Wait model to initialize
    Thread.sleep(2000)
    println("Done setting up H2O model")
    modelserver.ask(input).mapTo[Model.ModelReturn[ModelKeyDoubleValueArrayResult]]
      .map(data ⇒ {
        val recomendation = data.modelOutput
        println(s"Done serving recomendation model - $recomendation in ${data.modelServingStats.duration} ms")
        val keys = recomendation.keys
        assert(products.size == keys.size)
        assert(products == keys.map(_.toInt))
        ()
      })
    Thread.sleep(2000)
  }
}
