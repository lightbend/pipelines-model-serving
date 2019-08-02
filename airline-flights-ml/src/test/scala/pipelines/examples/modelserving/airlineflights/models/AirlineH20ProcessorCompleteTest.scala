package pipelines.examples.modelserving.airlineflights.models

import java.io.ByteArrayOutputStream

import akka.actor.ActorSystem
import akka.util.Timeout
import akka.pattern.ask
import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelType }
import com.lightbend.modelserving.model.actor.ModelServingActor
import com.lightbend.modelserving.model.h2o.H2OModel
import hex.genmodel.easy.prediction.BinomialModelPrediction
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.airlineflights.data.AirlineFlightRecord

import scala.concurrent.duration._

class AirlineH20ProcessorCompleteTest extends FlatSpec {

  implicit val system: ActorSystem = ActorSystem("ModelServing")
  implicit val executor = system.getDispatcher
  implicit val askTimeout = Timeout(30.seconds)

  val input = AirlineFlightRecord(
    year = 1990,
    month = 1,
    dayOfMonth = 3,
    dayOfWeek = 3,
    depTime = 1707,
    crsDepTime = 1630,
    arrTime = 1755,
    crsArrTime = 1723,
    uniqueCarrier = "US",
    flightNum = 29,
    tailNum = 0,
    actualElapsedTime = 48,
    crsElapsedTime = 53,
    airTime = 0,
    arrDelay = 32,
    depDelay = 37,
    origin = "CMH",
    destination = "IND",
    distance = 182,
    taxiIn = 0,
    taxiOut = 0,
    canceled = 0,
    cancellationCode = 0,
    diverted = 0,
    carrierDelay = 0,
    weatherDelay = 0,
    nASDelay = 0,
    securityDelay = 0,
    lateAircraftDelay = 0)

  private def getModel(): ModelDescriptor = {
    val is = this.getClass.getClassLoader.getResourceAsStream("airlines/models/mojo/gbm_pojo_test.zip")
    val buffer = new Array[Byte](1024)
    val content = new ByteArrayOutputStream()
    Stream.continually(is.read(buffer)).takeWhile(_ != -1).foreach(content.write(buffer, 0, _))
    val mojo = content.toByteArray
    new ModelDescriptor(
      modelName = "Airline model",
      description = "Mojo airline model complete test",
      modelType = ModelType.H2O,
      modelBytes = Some(mojo),
      modelSourceLocation = None)
  }

  "Processing of H2OModel" should "return label yes and probability around 0.6" in {

    val modelserver = system.actorOf(
      ModelServingActor.props[AirlineFlightRecord, BinomialModelPrediction](
        "airlines", AirlineFlightH2OModelFactory, () ⇒ new BinomialModelPrediction))
    // Wait for the actor to initialize and restore
    Thread.sleep(2000)

    modelserver.ask(getModel())
    // Wait model to initialize
    Thread.sleep(2000)
    println("Done setting up H2O model")
    modelserver.ask(input).mapTo[Model.ModelReturn[BinomialModelPrediction]]
      .map(data ⇒ {
        val bmp = data.modelOutput
        val (label, probability) = H2OModel.fromPrediction(bmp)
        println(s"Done serving airline model - label $label with probability $probability in ${data.modelServingStats.duration} ms")
        assert("YES" == label)
        assert(0.6 <= probability && probability <= 0.7)
        ()
      })
    Thread.sleep(2000)
  }
}
