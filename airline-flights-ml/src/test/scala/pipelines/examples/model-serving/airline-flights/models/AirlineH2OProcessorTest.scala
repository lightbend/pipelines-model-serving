package pipelines.examples.modelserving

import com.lightbend.modelserving.model.ModelToServe
import com.lightbend.modelserving.model.persistence.FilePersistence
import org.scalatest.FlatSpec
import pipelines.examples.data.{AirlineFlightRecord, AirlineFlightResult, ModelType}
import pipelines.examples.modelserving.airlineflightmodel.{AirlineFlightFactoryResolver, AirlineFlightH2OModel}

class AirlineH2OProcessorTest extends FlatSpec {

  val dtype = "airline"
  val name = "test name"
  val description = "test description"
  val input = AirlineFlightRecord(1990,1,3,3,1707,1630,1755,1723,"US", 29,0,48,53,0,32,37,"CMH","IND",182,0,0,0,0,0,0,0,0,0,0,dtype)

  "An instance AirlineFlightH2OModel" should "create correctly" in {

    val model = createModel()
    assert(model.isDefined)
    val result = verifyMode(model.get)
    assert(result)
  }

  "Model persistence" should "should work correctly" in {
    val original = createModel().get
    FilePersistence.saveState(dtype, original, name, description)
    ModelToServe.setResolver[AirlineFlightRecord, AirlineFlightResult](AirlineFlightFactoryResolver)
    val restored = FilePersistence.restoreState(dtype)
    assert(restored.isDefined)
    val result = verifyMode(restored.get._1.asInstanceOf[AirlineFlightH2OModel])
    assert(result)
    assert(restored.get._2 == name)
    assert(restored.get._3 == description)
  }

  private def createModel() : Option[AirlineFlightH2OModel] = {
    val is = this.getClass.getClassLoader.getResourceAsStream("airlines/models/mojo/gbm_pojo_test.zip")
    val mojo = new Array[Byte](is.available)
    is.read(mojo)
    val modelToServe = ModelToServe("airline","airline",ModelType.H2O.ordinal(), mojo, null, "airline")
    AirlineFlightH2OModel.create(modelToServe).asInstanceOf[Option[AirlineFlightH2OModel]]
  }

  private def verifyMode(model : AirlineFlightH2OModel) : Boolean = {
    val output = model.score(input)
    println(s"Model output $output")
    output.delayPredictionLabel == "YES"
  }
}
