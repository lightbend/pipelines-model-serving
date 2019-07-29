package pipelines.examples.modelserving.winequality.models.pmml

import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelFactory }
import com.lightbend.modelserving.model.pmml.PMMLModel
import org.jpmml.evaluator.Computable
import pipelines.examples.modelserving.winequality.WineModelCommon
import pipelines.examples.modelserving.winequality.data.{ WineRecord, WineResult }
import scala.collection.JavaConverters._

/**
 * PMML model implementation for wine data.
 */
class WinePMMLModel(descriptor: ModelDescriptor)
  extends PMMLModel[WineRecord, Double, WineResult](descriptor)
  with WineModelCommon {

  override protected def invokeModel(record: WineRecord): (String, Option[Double]) = {
    // Clear arguments (from previous run)
    arguments.clear()
    // Populate input based on record
    inputFields.asScala.foreach { field ⇒
      arguments.put(
        field.getName,
        field.prepare(record.get(field.getName.getValue.replaceAll(" ", "_"))))
    }

    // Calculate Output
    val result = evaluator.evaluate(arguments.asJava)

    // Prepare output
    val d = result.get(tname) match {
      case c: Computable ⇒ c.getResult.toString.toDouble
      case v: Any        ⇒ v.asInstanceOf[Double]
    }
    ("", Some(d))
  }
}

/**
 * Factory for wine data PMML model
 */
object WinePMMLModelFactory extends ModelFactory[WineRecord, WineResult] {

  def make(descriptor: ModelDescriptor): Either[String, Model[WineRecord, WineResult]] =
    if (descriptor == Model.noopModelDescriptor) Right(noopModel)
    else Right(new WinePMMLModel(descriptor))

  lazy val noopModel: Model[WineRecord, WineResult] =
    new WinePMMLModel(Model.noopModelDescriptor) with Model.NoopModel[WineRecord, Double, WineResult] {
      override protected def init(): Unit = {}
      override protected def invokeModel(record: WineRecord): (String, Option[Double]) =
        noopInvokeModel(record)
    }
}
