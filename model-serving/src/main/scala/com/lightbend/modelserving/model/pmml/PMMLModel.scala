package com.lightbend.modelserving.model.pmml

import java.io._
import java.util
import scala.collection._

import com.lightbend.modelserving.model.{ Model, ModelDescriptor }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

import org.dmg.pmml.{ FieldName, PMML }
import org.jpmml.evaluator.visitors._
import org.jpmml.evaluator._
import org.jpmml.model.PMMLUtil

/**
 * Abstract class for any PMML model processing. It has to be extended by the user
 * implement score method, based on his own model. Serializability here is required for Spark
 */
abstract class PMMLModel[RECORD, RESULT](val descriptor: ModelDescriptor)
  extends Model[RECORD, RESULT] with Serializable {

  assert(descriptor.modelBytes != None, s"Invalid descriptor ${descriptor.toRichString}")

  var arguments: mutable.Map[FieldName, FieldValue] = _
  var pmml: PMML = _
  var evaluator: ModelEvaluator[_ <: org.dmg.pmml.Model] = _
  var inputFields: util.List[InputField] = _
  var target: TargetField = _
  var tname: FieldName = _

  private def setup(): Unit = {
    arguments = mutable.Map[FieldName, FieldValue]()
    // Marshall PMML

    pmml = PMMLUtil.unmarshal(new ByteArrayInputStream(descriptor.modelBytes.get))
    // Optimize model// Optimize model
    PMMLModelBase.optimize(pmml)
    // Create and verify evaluator
    evaluator = ModelEvaluatorFactory.newInstance.newModelEvaluator(pmml)
    evaluator.verify()
    // Get input/target fields
    inputFields = evaluator.getInputFields
    target = evaluator.getTargetFields.get(0)
    tname = target.getName
  }

  setup()

  override def cleanup(): Unit = {}
}

object PMMLModelBase {

  // List of PMML optimizers (https://groups.google.com/forum/#!topic/jpmml/rUpv8hOuS3A)
  private val optimizers = Array(new ExpressionOptimizer, new FieldOptimizer, new PredicateOptimizer, new GeneralRegressionModelOptimizer, new NaiveBayesModelOptimizer, new RegressionModelOptimizer)

  /** Optimize a PMML model */
  def optimize(pmml: PMML) = this.synchronized {
    optimizers.foreach(opt ⇒
      try {
        opt.applyTo(pmml)
      } catch {
        case t: Throwable ⇒ {
          println(s"Error optimizing model for optimizer $opt")
          t.printStackTrace()
        }
      })
  }
}
