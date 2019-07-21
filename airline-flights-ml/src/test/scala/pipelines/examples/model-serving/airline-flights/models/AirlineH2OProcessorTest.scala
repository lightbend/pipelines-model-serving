package pipelines.examples.modelserving.airlineflights.models

import com.lightbend.modelserving.model.{ Model, ModelToServe, ModelType }
import com.lightbend.modelserving.model.persistence.FilePersistence
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }

class AirlineH2OProcessorTest extends FlatSpec {

  val dtype = "airline-model-state.dat"
  val name = "test name"
  val description = "test description"
  val input = AirlineFlightRecord(1990, 1, 3, 3, 1707, 1630, 1755, 1723, "US", 29, 0, 48, 53, 0, 32, 37, "CMH", "IND", 182, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, dtype)

  "createModel" should "create an instance of AirlineFlightH2OModel" in {

    val model = createModel() match {
      case Right(m) => m
      case Left(error) => fail(error)
    }
    assert("YES" == model.score(input).delayPredictionLabel)
  }

  "FilePersistence.saveState/restoreState" should "should save/restore the model using the file system" in {
    val original = createModel() match {
      case Right(m) => m
      case Left(error) => fail(error)
    }
    assert(Right(true) == FilePersistence.saveState(dtype, original, name, description))
    val (restoredModel, restoredName, restoredDescription) =
      FilePersistence.restoreState[AirlineFlightRecord, AirlineFlightResult](dtype) match {
        case Left(error) => fail(error)
        case Right((m, n, d)) => m match {
          case m2: Model[AirlineFlightRecord, AirlineFlightResult] => (m2, n, d)
          case _ => fail(s"Unexpected model kind: $m (name = $n, description = $d)")
        }
      }
    assert("YES" == restoredModel.score(input).delayPredictionLabel)
    assert(name == restoredName)
    assert(description == restoredDescription)
  }

  private def createModel(): Either[String, Model[AirlineFlightRecord, AirlineFlightResult]] = {
    val is = this.getClass.getClassLoader.getResourceAsStream("airlines/models/mojo/gbm_pojo_test.zip")
    val mojo = new Array[Byte](is.available)
    is.read(mojo)
    val modelToServe = ModelToServe("airline", "airline", ModelType.H2O.ordinal(), mojo, null, "airline")
    AirlineFlightH2OModel.create(modelToServe)
  }
}
