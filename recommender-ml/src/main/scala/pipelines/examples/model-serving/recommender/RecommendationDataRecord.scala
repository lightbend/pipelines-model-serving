package pipelines.examples.modelserving.recommender

import pipelines.examples.modelserving.recommender.data.RecommenderRecord
import com.lightbend.modelserving.model.DataToServe

case class RecommendationDataRecord(record: RecommenderRecord) extends DataToServe[RecommenderRecord] {
  def getType: String = record.dataType
}
