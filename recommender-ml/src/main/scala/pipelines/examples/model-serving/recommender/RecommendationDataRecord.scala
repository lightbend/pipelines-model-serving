package pipelines.examples.modelserving.recommender

import pipelines.examples.modelserving.recommender.data.RecommenderRecord
import com.lightbend.modelserving.model.DataToServe

case class RecommendationDataRecord(record: RecommenderRecord) extends DataToServe {
  def getType: String = record.dataType
  def getRecord: AnyVal = record.asInstanceOf[AnyVal]
}
