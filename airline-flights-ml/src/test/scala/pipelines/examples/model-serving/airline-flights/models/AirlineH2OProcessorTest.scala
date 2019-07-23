package pipelines.examples.modelserving.airlineflights.models

import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelManager, ModelType }
import com.lightbend.modelserving.model.persistence.FilePersistence
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import pipelinesx.test.OutputInterceptor

class AirlineH2OProcessorTest extends FlatSpec with OutputInterceptor {

  val filePath = "airlines/models/mojo/gbm_pojo_test.zip"
  val savePath = "airline-model-state.dat"
  val name = "test name"
  val description = "test description"
  val input = AirlineFlightRecord(1990, 1, 3, 3, 1707, 1630, 1755, 1723, "US", 29, 0, 48, 53, 0, 32, 37, "CMH", "IND", 182, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, savePath)

  val modelManager =
    new ModelManager[AirlineFlightRecord, AirlineFlightResult](AirlineFlightFactoryResolver)
  val fp = FilePersistence[AirlineFlightRecord, AirlineFlightResult](modelManager)

  "Loading a valid model for the first time" should "succeed" in {
    ignoreOutput {
      val model = createModel("airline") match {
        case Right(m)    ⇒ m
        case Left(error) ⇒ fail(error)
      }
      model.score(input) match {
        case Left(error)   ⇒ fail(error)
        case Right(result) ⇒ assert("YES" == result.delayPredictionLabel)
      }
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
      assert(fp.stateExists("airline") == true)
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
      restoredModel.score(input) match {
        case Left(error)   ⇒ fail(error)
        case Right(result) ⇒ assert("YES" == result.delayPredictionLabel)
      }
    }
  }

  private def createModel(name: String): Either[String, Model[AirlineFlightRecord, AirlineFlightResult]] = {
    val is = this.getClass.getClassLoader.getResourceAsStream(filePath)
    val mojo = new Array[Byte](is.available)
    is.read(mojo)
    val descriptor = ModelDescriptor(
      name = name,
      description = "airline H2O model",
      dataType = "airline",
      modelType = ModelType.H2O,
      modelBytes = Some(mojo),
      modelSourceLocation = Some(filePath))
    AirlineFlightH2OModel.create(descriptor)
  }
}
