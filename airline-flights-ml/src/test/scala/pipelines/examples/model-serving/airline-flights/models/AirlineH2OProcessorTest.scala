package pipelines.examples.modelserving.airlineflights.models

import com.lightbend.modelserving.model.{ ModelToServe, ModelType }
import com.lightbend.modelserving.model.persistence.FilePersistence
import org.scalatest.FlatSpec
import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }

class AirlineH2OProcessorTest extends FlatSpec {

  val dtype = "airline-model-state.dat"
  val name = "test name"
  val description = "test description"
  val input = AirlineFlightRecord(1990, 1, 3, 3, 1707, 1630, 1755, 1723, "US", 29, 0, 48, 53, 0, 32, 37, "CMH", "IND", 182, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, dtype)

  "createModel" should "create an instance of AirlineFlightH2OModel" in {

    val model = createModel()
    assert(model.isDefined)
    assert("YES" == model.get.score(input).delayPredictionLabel)
  }

  "FilePersistence.saveState/restoreState" should "should save/restore the model using the file system" in {
    val original = createModel().get
    FilePersistence.saveState(dtype, original, name, description)
    ModelToServe.setResolver[AirlineFlightRecord, AirlineFlightResult](AirlineFlightFactoryResolver)
    val restored = FilePersistence.restoreState(dtype)
    assert(restored.isDefined)
    assert("YES" == restored.get._1.asInstanceOf[AirlineFlightH2OModel].score(input).delayPredictionLabel)
    assert(restored.get._2 == name)
    assert(restored.get._3 == description)
  }

  private def createModel(): Option[AirlineFlightH2OModel] = {
    val is = this.getClass.getClassLoader.getResourceAsStream("airlines/models/mojo/gbm_pojo_test.zip")
    val mojo = new Array[Byte](is.available)
    is.read(mojo)
    val modelToServe = ModelToServe("airline", "airline", ModelType.H2O.ordinal(), mojo, null, "airline")
    AirlineFlightH2OModel.create(modelToServe).asInstanceOf[Option[AirlineFlightH2OModel]]
  }
}
