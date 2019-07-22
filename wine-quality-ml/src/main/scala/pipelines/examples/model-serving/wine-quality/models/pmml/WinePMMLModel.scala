package pipelines.examples.modelserving.winequality.models.pmml

import com.lightbend.modelserving.model.{ Model, ModelFactory, ModelMetadata }
import com.lightbend.modelserving.model.pmml.PMMLModel
import org.jpmml.evaluator.Computable
import pipelines.examples.modelserving.winequality.data.WineRecord

import scala.collection.JavaConverters._

/**
 * PMML model implementation for wine data.
 */
class WinePMMLModel(metadata: ModelMetadata)
  extends PMMLModel[WineRecord, Double](metadata) {

  /** Scoring (using PMML evaluator) */
  override def score(input: WineRecord): Either[String, Double] = {
    // Clear arguments (from previous run)
    arguments.clear()
    // Populate input based on record
    inputFields.asScala.foreach(field ⇒ {
      arguments.put(field.getName, field.prepare(input.get(field.getName.getValue.replaceAll(" ", "_"))))
    })

    // Calculate Output
    val result = evaluator.evaluate(arguments.asJava)

    // Prepare output
    val d = result.get(tname) match {
      case c: Computable ⇒ c.getResult.toString.toDouble
      case v: Any ⇒ v.asInstanceOf[Double]
    }
    Right(d)
  }
}

/**
 * Factory for wine data PMML model
 */
object WinePMMLModel extends ModelFactory[WineRecord, Double] {

  val modelName = "WinePMMLModel"

  def make(metadata: ModelMetadata): Model[WineRecord, Double] =
    new WinePMMLModel(metadata)
}
