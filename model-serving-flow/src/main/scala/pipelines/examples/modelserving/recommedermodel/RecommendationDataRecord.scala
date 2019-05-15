package pipelines.examples.modelserving.recommedermodel

import com.lightbend.modelserving.model.DataToServe
import pipelines.examples.data.RecommenderRecord

case class RecommendationDataRecord(record: RecommenderRecord) extends DataToServe {
  def getType: String = record.dataType
  def getRecord: AnyVal = record.asInstanceOf[AnyVal]
}
