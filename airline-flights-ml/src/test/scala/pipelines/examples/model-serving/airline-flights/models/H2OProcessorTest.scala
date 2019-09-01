package pipelines.examples.modelserving.airlineflights.models

import java.io.ByteArrayOutputStream

import pipelinesx.modelserving.model.h2o.H2OModel
import pipelinesx.modelserving.model.{ Model, ModelDescriptor, ModelServingStats, ModelType }
import hex.genmodel.easy.prediction.BinomialModelPrediction
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.airlineflights.data.AirlineFlightRecord

class H2OProcessorTest extends FlatSpec {

  val dtype = "airline"
  val name = "test name"
  val description = "test description"
  val modelstats = ModelServingStats.unknown

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

  "An instance AirlineFlightH2OModel" should "create correctly" in {

    createModel() match {
      case Right(model) ⇒
        // Model created successfully
        val result = model.score(input, modelstats).modelOutput
        val (label, probability) = H2OModel.fromPrediction(result)
        assert("YES" == label)
        assert(0.6 <= probability && probability <= 0.7)
        ()
      case Left(error) ⇒
        println(s"  Failed to instantiate the model: $error")
        assert(false)
    }
  }

  private def createModel(): Either[String, Model[AirlineFlightRecord, BinomialModelPrediction]] = {
    val is = this.getClass.getClassLoader.getResourceAsStream("airlines/models/mojo/gbm_pojo_test.zip")
    val buffer = new Array[Byte](1024)
    val content = new ByteArrayOutputStream()
    Stream.continually(is.read(buffer)).takeWhile(_ != -1).foreach(content.write(buffer, 0, _))
    val mojo = content.toByteArray
    val model = new ModelDescriptor(
      modelName = "Airline model",
      description = "Mojo airline model",
      modelType = ModelType.H2O,
      modelBytes = Some(mojo),
      modelSourceLocation = None)

    AirlineFlightH2OModelFactory.create(model)
  }
}
