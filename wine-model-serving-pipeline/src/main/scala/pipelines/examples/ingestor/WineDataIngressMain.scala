package pipelines.examples.ingestor

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Source, Sink }
import pipelines.examples.data.WineRecord
import pipelines.ingress.RecordsReader
import pipelines.config.ConfigUtil
import pipelines.config.ConfigUtil.implicits._
import scala.concurrent.duration._
import pipelines.logging.{ Logger, LoggingUtil }

/**
 * Test program for [[WineDataIngressUtil]]; reads wine records and prints them.
 */
object WineDataIngressMain {

  /** For testing purposes. */
  def main(args: Array[String]): Unit = {
    var count = if (args.length > 0) args(0).toInt else 1000
    println(s"printing $count records")
    println(s"frequency (seconds): ${WineDataIngressUtil.dataFrequencyMilliseconds}")
    implicit val system = ActorSystem("RecommenderDataIngressMain")
    implicit val mat = ActorMaterializer()
    val source = WineDataIngressUtil.makeSource(
      WineDataIngressUtil.rootConfigKey,
      WineDataIngressUtil.dataFrequencyMilliseconds)
    source.runWith {
      Sink.foreach { line ⇒
        println(line.toString)
        count -= 1
        if (count == 0) {
          println("Finished!")
          sys.exit(0)
        }
      }
    }
  }
}

