package pipelines.examples.modelserving.airlineflights.models

import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelType, ServingResult }
import com.lightbend.modelserving.model.persistence.FilePersistence
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import pipelinesx.test.OutputInterceptor

// TODO: Most of this logic is really about FilePersistence, so move this logic to
// that project...
class AirlineH2OProcessorTest extends FlatSpec with OutputInterceptor {

  val filePath = "airlines/models/mojo/gbm_pojo_test.zip"
  val savePath = "airline-model-state.dat"
  val name = "test name"
  val description = "test description"
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

  val fp = FilePersistence[AirlineFlightRecord, AirlineFlightResult](
    AirlineFlightH2OModelFactory, "test-persistence")

  def assertServingResult(servingResult: ServingResult[AirlineFlightResult]): Unit = {
    assert("" == servingResult.errors)
    servingResult.result match {
      case None ⇒ fail("None result")
      case Some(res) ⇒
        val probability = res.delayPredictionProbability
        assert("YES" == res.delayPredictionLabel)
        assert(0.6 <= probability && probability <= 0.7)
    }
  }

  "Loading a valid model for the first time" should "succeed" in {
    ignoreOutput {
      val model = createModel("airline") match {
        case Right(m)    ⇒ m
        case Left(error) ⇒ fail(error)
      }
      val servingResult = model.score(input)
      assertServingResult(servingResult)
    }
  }

  "FilePersistence.stateExists" should "return false if the model hasn't been saved to the file system" in {
    assert(fp.stateExists("foobar") == false)
  }

  "FilePersistence.stateExists" should "return true if the model has been saved to the file system" in {
    ignoreOutput {
      val original = createModel("airline") match {
        case Right(m)    ⇒ m
        case Left(error) ⇒ fail(error)
      }

      assert(Right(true) == fp.saveState(original, savePath))
      assert(fp.stateExists(savePath) == true, s"${fp.statePath("airline")} should exist, but doesn't!")
    }
  }

  "FilePersistence.saveState/restoreState" should "should save/restore the model using the file system" in {
    ignoreOutput {
      val original = createModel("airline") match {
        case Right(m)    ⇒ m
        case Left(error) ⇒ fail(error)
      }

      assert(Right(true) == fp.saveState(original, savePath))
      val restoredModel = fp.restoreState(savePath) match {
        case Left(error) ⇒ fail(error)
        case Right(m) ⇒ m match {
          case m2: Model[AirlineFlightRecord, AirlineFlightResult] ⇒ m2
          case _                                                   ⇒ fail(s"Unexpected model kind: $m")
        }
      }
      assert(original.descriptor == restoredModel.descriptor)
      assertServingResult(restoredModel.score(input))
    }
  }

  private def createModel(name: String): Either[String, Model[AirlineFlightRecord, AirlineFlightResult]] = {
    val is = this.getClass.getClassLoader.getResourceAsStream(filePath)
    val mojo = new Array[Byte](is.available)
    is.read(mojo)
    val descriptor = ModelDescriptor(
      name = name,
      description = "airline H2O model",
      modelType = ModelType.H2O,
      modelBytes = Some(mojo),
      modelSourceLocation = Some(filePath))
    AirlineFlightH2OModelFactory.create(descriptor)
  }
}
