package pipelines.examples.modelserving.winequality.models.pmml

import pipelinesx.modelserving.model.{ Model, ModelDescriptor, ModelFactory }
import pipelinesx.modelserving.model.pmml.PMMLModel
import org.jpmml.evaluator.Computable
import pipelines.examples.modelserving.winequality.data.WineRecord
import scala.collection.JavaConverters._

/**
 * PMML model implementation for wine data.
 */
class WinePMMLModel(descriptor: ModelDescriptor)
  extends PMMLModel[WineRecord, Double](descriptor)(() ⇒ 0.0) {

  override protected def invokeModel(record: WineRecord): Either[String, Double] = {
    // Clear arguments (from previous run)
    arguments.clear()
    // Populate input based on record
    inputFields.asScala.foreach(field ⇒
      arguments.put(
        field.getName,
        field.prepare(record.get(field.getName.getValue.replaceAll(" ", "_"))))
    )

    // Calculate Output
    try {
      val result = evaluator.evaluate(arguments.asJava)
      // Prepare output
      val d = result.get(tname) match {
        case c: Computable ⇒ c.getResult.toString.toDouble
        case v: Any        ⇒ v.asInstanceOf[Double]
      }
      Right(d)
    } catch {
      case t: Throwable ⇒
        t.printStackTrace()
        Left(t.getMessage)
    }
  }
}

/**
 * Factory for wine data PMML model
 */
object WinePMMLModelFactory extends ModelFactory[WineRecord, Double] {

  def make(descriptor: ModelDescriptor): Either[String, Model[WineRecord, Double]] =
    Right(new WinePMMLModel(descriptor))
}
