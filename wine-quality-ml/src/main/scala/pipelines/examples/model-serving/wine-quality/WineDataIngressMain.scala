package pipelines.examples.modelserving.winequality

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink

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
    println("Should never get here...")
  }
}

