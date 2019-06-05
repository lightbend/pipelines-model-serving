package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Random

/**
  * Ingress of data for recommendations. In this case, every second we
  * load and send downstream one record that is randomly generated.
  */
class RecommenderDataIngress extends SourceIngress[RecommenderRecord] {

  val generator = Random

  override def createLogic = new SourceIngressLogic() {

    def source: Source[RecommenderRecord, NotUsed] = {
      Source.repeat(NotUsed)
        .map(_ ⇒ getRecommenderRecord())
        .throttle(1, 1.seconds) // "dribble" them out
    }
  }

  def getRecommenderRecord(): RecommenderRecord = {
    val user = generator.nextInt(1000).toLong
    val nprods = generator.nextInt(30)
    val products = new ListBuffer[Long]()
    0 to nprods foreach { _ ⇒ products += generator.nextInt(300).toLong }
    new RecommenderRecord(user, products, "recommender")

  }
}

object RecommenderDataIngress {
  def main(args: Array[String]): Unit = {
    val ingress = new RecommenderDataIngress()
    while (true)
      println(ingress.getRecommenderRecord())
  }
}
